// =============================================================
// Systematic Problem Scouting — Neo4j Graph Schema
// 11 node types · 11 uniqueness constraints · 15 indexes
// Apply with: python graph/schema.py
// =============================================================


// -------------------------------------------------------------
// UNIQUENESS CONSTRAINTS (one per node type, on primary key)
// -------------------------------------------------------------

CREATE CONSTRAINT bm_id_unique IF NOT EXISTS
  FOR (n:BusinessModel) REQUIRE n.bim_id IS UNIQUE;

CREATE CONSTRAINT vector_id_unique IF NOT EXISTS
  FOR (n:TransformationVector) REQUIRE n.vector_id IS UNIQUE;

CREATE CONSTRAINT scalar_id_unique IF NOT EXISTS
  FOR (n:Scalar) REQUIRE n.scalar_id IS UNIQUE;

CREATE CONSTRAINT company_id_unique IF NOT EXISTS
  FOR (n:Company) REQUIRE n.company_id IS UNIQUE;

CREATE CONSTRAINT industry_id_unique IF NOT EXISTS
  FOR (n:Industry) REQUIRE n.industry_id IS UNIQUE;

CREATE CONSTRAINT technology_id_unique IF NOT EXISTS
  FOR (n:Technology) REQUIRE n.tech_id IS UNIQUE;

CREATE CONSTRAINT hypothesis_id_unique IF NOT EXISTS
  FOR (n:DisruptionHypothesis) REQUIRE n.hyp_id IS UNIQUE;

CREATE CONSTRAINT evidence_id_unique IF NOT EXISTS
  FOR (n:Evidence) REQUIRE n.evidence_id IS UNIQUE;

CREATE CONSTRAINT evaluation_id_unique IF NOT EXISTS
  FOR (n:Evaluation) REQUIRE n.eval_id IS UNIQUE;

CREATE CONSTRAINT review_item_id_unique IF NOT EXISTS
  FOR (n:HumanReviewItem) REQUIRE n.queue_id IS UNIQUE;

CREATE CONSTRAINT compression_log_id_unique IF NOT EXISTS
  FOR (n:CompressionLog) REQUIRE n.log_id IS UNIQUE;


// -------------------------------------------------------------
// INDEXES (on frequently-queried properties)
// -------------------------------------------------------------

CREATE INDEX bm_name IF NOT EXISTS
  FOR (n:BusinessModel) ON (n.name);

CREATE INDEX bm_status IF NOT EXISTS
  FOR (n:BusinessModel) ON (n.status);

CREATE INDEX company_name IF NOT EXISTS
  FOR (n:Company) ON (n.name);

CREATE INDEX company_monitoring_status IF NOT EXISTS
  FOR (n:Company) ON (n.monitoring_status);

CREATE INDEX industry_name IF NOT EXISTS
  FOR (n:Industry) ON (n.name);

CREATE INDEX technology_name IF NOT EXISTS
  FOR (n:Technology) ON (n.name);

CREATE INDEX technology_tracking_status IF NOT EXISTS
  FOR (n:Technology) ON (n.tracking_status);

CREATE INDEX hypothesis_status IF NOT EXISTS
  FOR (n:DisruptionHypothesis) ON (n.status);

CREATE INDEX hypothesis_confidence IF NOT EXISTS
  FOR (n:DisruptionHypothesis) ON (n.confidence_score);

CREATE INDEX hypothesis_updated_at IF NOT EXISTS
  FOR (n:DisruptionHypothesis) ON (n.updated_at);

CREATE INDEX evidence_source_type IF NOT EXISTS
  FOR (n:Evidence) ON (n.source_type);

CREATE INDEX evidence_status IF NOT EXISTS
  FOR (n:Evidence) ON (n.status);

CREATE INDEX evaluation_alert_status IF NOT EXISTS
  FOR (n:Evaluation) ON (n.alert_status);

CREATE INDEX review_item_status IF NOT EXISTS
  FOR (n:HumanReviewItem) ON (n.status);

CREATE INDEX review_item_priority IF NOT EXISTS
  FOR (n:HumanReviewItem) ON (n.priority_score);


// -------------------------------------------------------------
// RELATIONSHIP TYPES (documented here — created when data loads)
// -------------------------------------------------------------
// (BusinessModel)        -[:HAS_TRANSITION]->  (BusinessModel)
// (TransformationVector) -[:FROM_BIM]->         (BusinessModel)
// (TransformationVector) -[:TO_BIM]->           (BusinessModel)
// (Scalar)               -[:DRIVES]->           (TransformationVector)
// (Technology)           -[:IMPACTS]->          (Scalar)
// (Company)              -[:CLASSIFIES]->       (BusinessModel)
// (Company)              -[:BELONGS_TO]->       (Industry)
// (Technology)           -[:TRIGGERS]->         (DisruptionHypothesis)
// (DisruptionHypothesis) -[:TARGETS]->          (Company)
// (DisruptionHypothesis) -[:PREDICTS]->         (TransformationVector)
// (DisruptionHypothesis) -[:ADVANCES_TO]->      (Evaluation)
// (Evaluation)           -[:MONITORS]->         (Technology)
// (Evidence)             -[:DEMONSTRATES]->     (DisruptionHypothesis)
// (Evidence)             -[:SUPPORTS]->         (DisruptionHypothesis)
// (HumanReviewItem)      -[:REVIEWS]->          (DisruptionHypothesis)
