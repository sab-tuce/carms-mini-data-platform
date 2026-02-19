-- Basic full-text search index on section_text
CREATE INDEX IF NOT EXISTS idx_sections_fts
ON program_description_sections
USING GIN (to_tsvector('english', COALESCE(section_text, '')));
