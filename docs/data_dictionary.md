# Data Dictionary (Iteration 1503)
## Tables / Files
- **discipline**: 37 rows × 2 cols
- **program_master**: 815 rows × 11 cols
- **x_section**: 815 rows × 21 cols

## Columns
### discipline
- `discipline_id`
- `discipline`

### program_master
- `Unnamed: 0`
- `discipline_id`
- `discipline_name`
- `school_id`
- `school_name`
- `program_stream_id`
- `program_stream_name`
- `program_site`
- `program_stream`
- `program_name`
- `program_url`

### x_section
- `Unnamed: 0`
- `document_id`
- `source`
- `n_program_description_sections`
- `program_name`
- `match_iteration_name`
- `program_contracts`
- `general_instructions`
- `supporting_documentation_information`
- `review_process`
- `interviews`
- `selection_criteria`
- `program_highlights`
- `program_curriculum`
- `training_sites`
- `additional_information`
- `return_of_service`
- `faq`
- `summary_of_changes`
- `match_iteration_id`
- `program_description_id`

## Notes
- `Unnamed: 0` is an old Excel index column → should be dropped.
- `data_profile.json` contains a machine-readable summary.
