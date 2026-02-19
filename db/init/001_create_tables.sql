-- Core lookup
CREATE TABLE IF NOT EXISTS disciplines (
  discipline_id INTEGER PRIMARY KEY,
  discipline TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schools (
  school_id INTEGER PRIMARY KEY,
  school_name TEXT NOT NULL
);

-- Master list of programs/streams
CREATE TABLE IF NOT EXISTS program_streams (
  program_stream_id INTEGER PRIMARY KEY,
  discipline_id INTEGER NOT NULL REFERENCES disciplines(discipline_id),
  school_id INTEGER NOT NULL REFERENCES schools(school_id),

  discipline_name TEXT,
  school_name TEXT,

  program_stream_name TEXT,
  program_site TEXT,
  program_stream TEXT,
  program_name TEXT,

  program_url TEXT UNIQUE NOT NULL,
  match_iteration_id INTEGER NOT NULL DEFAULT 1503
);

-- One description per program stream (joined via URL from x_section.source)
CREATE TABLE IF NOT EXISTS program_descriptions (
  program_description_id INTEGER PRIMARY KEY,
  program_stream_id INTEGER NOT NULL UNIQUE REFERENCES program_streams(program_stream_id),
  source_url TEXT UNIQUE NOT NULL,

  document_id TEXT,
  match_iteration_id INTEGER,
  match_iteration_name TEXT,
  program_name TEXT,
  n_program_description_sections INTEGER
);

-- Normalized sections (instead of 21 wide columns)
CREATE TABLE IF NOT EXISTS program_description_sections (
  id BIGSERIAL PRIMARY KEY,
  program_description_id INTEGER NOT NULL REFERENCES program_descriptions(program_description_id) ON DELETE CASCADE,
  section_name TEXT NOT NULL,
  section_text TEXT
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_program_streams_discipline ON program_streams(discipline_id);
CREATE INDEX IF NOT EXISTS idx_program_streams_school ON program_streams(school_id);
CREATE INDEX IF NOT EXISTS idx_sections_name ON program_description_sections(section_name);
