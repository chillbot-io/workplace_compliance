"""
pipeline/entity_resolution.py — Splink probabilistic record linkage.

Clusters OSHA inspection records that refer to the same physical employer.
Maintains stable employer_id UUIDs across pipeline runs via cluster_id_mapping.

Run after dbt Silver models, before dbt Gold models.

Usage:
    python pipeline/entity_resolution.py
"""

import os
import uuid

import duckdb
import pandas as pd
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/duckdb/employer_compliance.duckdb")


def run_deduplication():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("SET memory_limit='20GB'")
    con.execute("SET threads=8")

    # Ensure cluster tables exist in DuckDB
    con.execute("""
        CREATE TABLE IF NOT EXISTS cluster_id_mapping (
            employer_id VARCHAR NOT NULL,
            cluster_id VARCHAR NOT NULL,
            superseded_by VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS employer_clusters (
            unique_id VARCHAR,
            cluster_id VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS review_queue (
            record_id_left VARCHAR,
            record_id_right VARCHAR,
            match_probability DOUBLE
        )
    """)

    # Check we have data
    count = con.execute("SELECT COUNT(*) FROM osha_inspection_norm").fetchone()[0]
    if count == 0:
        print("ERROR: osha_inspection_norm is empty — nothing to deduplicate")
        con.close()
        return

    print(f"Starting entity resolution on {count} OSHA inspection records...")

    # Check if address_key exists (parse_addresses.py may not have run yet)
    columns = [row[0] for row in con.execute("DESCRIBE osha_inspection_norm").fetchall()]
    has_address_key = "address_key" in columns

    # Prepare the input table — one row per establishment with dedup fields
    address_key_select = "address_key" if has_address_key else "NULL AS address_key"
    con.execute(f"""
        CREATE OR REPLACE TABLE er_input AS
        SELECT
            activity_nr AS unique_id,
            name_normalized,
            SUBSTR(name_normalized, 1, 4) AS name_prefix,
            zip5,
            site_state,
            naics_4digit,
            {address_key_select}
        FROM osha_inspection_norm
        WHERE name_normalized IS NOT NULL
          AND name_normalized != ''
    """)

    er_count = con.execute("SELECT COUNT(*) FROM er_input").fetchone()[0]
    print(f"ER input: {er_count} records (after filtering nulls)")

    # Configure Splink
    settings = SettingsCreator(
        link_type="dedupe_only",
        blocking_rules_to_generate_predictions=[
            block_on("zip5"),
            block_on("site_state", "name_prefix"),
            block_on("name_prefix", "naics_4digit"),  # multi-geography employers
        ],
        comparisons=[
            cl.ExactMatch("address_key").configure(term_frequency_adjustments=True),
            cl.JaroWinklerAtThresholds("name_normalized", [0.92, 0.80]),
            cl.ExactMatch("naics_4digit"),
            cl.ExactMatch("site_state"),
        ],
    )

    db_api = DuckDBAPI(connection=con)
    linker = Linker(con.table("er_input"), settings, db_api=db_api)

    # Train the model (unsupervised — no labeled data needed)
    print("Training Splink model (unsupervised EM)...")
    linker.training.estimate_u_using_random_sampling(max_pairs=500_000)
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("zip5"), fix_u_probabilities=False
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("site_state", "name_prefix"), fix_u_probabilities=False
    )

    # Predict matches and cluster
    print("Predicting matches (threshold=0.80)...")
    predictions = linker.inference.predict(threshold_match_probability=0.80)

    print("Clustering (threshold=0.85)...")
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions, threshold_match_probability=0.85
    )
    clusters_df = clusters.as_pandas_dataframe()

    print(f"Splink produced {clusters_df['cluster_id'].nunique()} clusters from {len(clusters_df)} records")

    # Save clusters to DuckDB
    con.register("clusters_df", clusters_df)

    # Snapshot previous clusters before overwriting (for stable ID mapping)
    try:
        con.execute("CREATE OR REPLACE TABLE employer_clusters_prev AS SELECT * FROM employer_clusters")
    except Exception:
        # First run — no previous clusters
        pass

    con.execute("CREATE OR REPLACE TABLE employer_clusters AS SELECT * FROM clusters_df")

    # Map clusters to stable employer_id UUIDs
    update_cluster_mapping(con)

    # Route borderline pairs (0.80-0.85) to review_queue
    route_borderline_pairs(con, predictions)

    # Clean up
    try:
        con.execute("DROP TABLE IF EXISTS employer_clusters_prev")
    except Exception:
        pass

    con.close()
    print("Entity resolution complete.")


def update_cluster_mapping(con):
    """Map Splink's transient cluster_ids to stable employer_id UUIDs."""

    # Get existing mappings (if any — first run will be empty)
    try:
        existing = con.execute("""
            SELECT employer_id, cluster_id FROM cluster_id_mapping
            WHERE superseded_by IS NULL
        """).df()
    except Exception:
        existing = pd.DataFrame(columns=["employer_id", "cluster_id"])

    # Get new clusters with member counts
    new_clusters = con.execute("""
        SELECT cluster_id, COUNT(*) as member_count
        FROM employer_clusters
        GROUP BY cluster_id
    """).df()

    existing_map = dict(zip(existing["cluster_id"].astype(str), existing["employer_id"])) if not existing.empty else {}
    claimed = set()
    mappings = []

    # Sort largest first — largest cluster gets priority for ID inheritance
    new_clusters = new_clusters.sort_values("member_count", ascending=False)

    for _, row in new_clusters.iterrows():
        cid = str(row["cluster_id"])

        # Direct match — same cluster_id as before
        if cid in existing_map and existing_map[cid] not in claimed:
            eid = existing_map[cid]
            mappings.append({"employer_id": eid, "cluster_id": cid})
            claimed.add(eid)
        else:
            # Check overlap via member records
            try:
                overlap = con.execute("""
                    SELECT m.employer_id, COUNT(*) as overlap_count
                    FROM cluster_id_mapping m
                    JOIN employer_clusters_prev ec_old ON CAST(m.cluster_id AS VARCHAR) = CAST(ec_old.cluster_id AS VARCHAR)
                    JOIN employer_clusters ec_new ON ec_old.unique_id = ec_new.unique_id
                    WHERE CAST(ec_new.cluster_id AS VARCHAR) = ?
                      AND m.superseded_by IS NULL
                    GROUP BY m.employer_id
                    ORDER BY overlap_count DESC
                    LIMIT 1
                """, [cid]).df()
            except Exception:
                overlap = pd.DataFrame()

            if not overlap.empty and overlap.iloc[0]["employer_id"] not in claimed:
                eid = overlap.iloc[0]["employer_id"]
                mappings.append({"employer_id": eid, "cluster_id": cid})
                claimed.add(eid)
            else:
                # New cluster — new UUID
                eid = str(uuid.uuid4())
                mappings.append({"employer_id": eid, "cluster_id": cid})
                claimed.add(eid)

    if mappings:
        mapping_df = pd.DataFrame(mappings)
        con.register("mapping_df", mapping_df)
        con.execute("DELETE FROM cluster_id_mapping WHERE superseded_by IS NULL")
        con.execute("""
            INSERT INTO cluster_id_mapping (employer_id, cluster_id)
            SELECT employer_id, cluster_id FROM mapping_df
        """)
        print(f"cluster_id_mapping updated: {len(mappings)} mappings ({len(existing_map)} previous)")


def route_borderline_pairs(con, predictions):
    """Route pairs with match_probability between 0.80-0.85 to review_queue."""
    pred_df = predictions.as_pandas_dataframe()
    borderline = pred_df[
        (pred_df["match_probability"] >= 0.80) & (pred_df["match_probability"] < 0.85)
    ]

    if borderline.empty:
        print("No borderline pairs to route to review_queue")
        return

    # Limit to 500 most uncertain pairs per run
    borderline = borderline.nsmallest(500, "match_probability")

    review_rows = []
    for _, row in borderline.iterrows():
        review_rows.append({
            "record_id_left": str(row.get("unique_id_l", row.get("id_l", ""))),
            "record_id_right": str(row.get("unique_id_r", row.get("id_r", ""))),
            "match_probability": float(row["match_probability"]),
        })

    review_df = pd.DataFrame(review_rows)
    con.register("review_df", review_df)
    con.execute("""
        INSERT INTO review_queue (record_id_left, record_id_right, match_probability)
        SELECT record_id_left, record_id_right, match_probability FROM review_df
    """)
    print(f"Routed {len(review_rows)} borderline pairs to review_queue")


if __name__ == "__main__":
    run_deduplication()
