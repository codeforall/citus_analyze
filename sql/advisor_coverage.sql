DROP TABLE IF EXISTS pg_temp._advisor_coverage;
CREATE TEMP TABLE _advisor_coverage (probe text, failed bigint, missing bigint);
DO $coverage$
DECLARE
  probe record;
  failed_count bigint;
  missing_count bigint;
BEGIN
  FOR probe IN
    SELECT relation.oid, relation.relname
    FROM pg_class relation
    WHERE relation.relnamespace = pg_my_temp_schema() AND relation.relkind = 'r'
      AND relation.relname LIKE '%\_raw' ESCAPE '\'
      AND EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid=relation.oid AND attname='success')
  LOOP
    EXECUTE format('SELECT count(*) FROM pg_temp.%I WHERE success IS NOT TRUE', probe.relname) INTO failed_count;
    missing_count := 0;
    IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid=probe.oid AND attname='nodeid') THEN
      EXECUTE format('SELECT count(*) FROM pg_dist_node node WHERE isactive AND noderole=''primary'' AND NOT EXISTS (SELECT 1 FROM pg_temp.%I raw WHERE raw.nodeid=node.nodeid)', probe.relname) INTO missing_count;
    END IF;
    INSERT INTO _advisor_coverage VALUES (probe.relname, failed_count, missing_count);
  END LOOP;
END
$coverage$;
SELECT format('INCOMPLETE : probe %s: %s failed node(s), %s missing primary node(s); health only partially assessed', probe, failed, missing) AS finding
FROM _advisor_coverage WHERE failed > 0 OR missing > 0;
DROP TABLE pg_temp._advisor_coverage;