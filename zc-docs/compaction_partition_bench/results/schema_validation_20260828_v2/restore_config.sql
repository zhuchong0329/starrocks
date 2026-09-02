UPDATE information_schema.be_configs SET VALUE = '86400' WHERE NAME = 'base_compaction_interval_seconds_since_last_operation';
UPDATE information_schema.be_configs SET VALUE = '60' WHERE NAME = 'base_compaction_check_interval_seconds';
