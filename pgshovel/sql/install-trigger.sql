CREATE TRIGGER _pgshovel_${application}_${group} AFTER INSERT OR UPDATE OR DELETE ON ${table} FOR EACH ROW EXECUTE PROCEDURE ${schema}.capture("${group}", "${alias}");
