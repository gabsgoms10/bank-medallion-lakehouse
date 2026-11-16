-- ============================================================================
-- 00_setup — schemas under the workspace catalog (Unity Catalog)
-- Idempotent: safe to re-run.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;
