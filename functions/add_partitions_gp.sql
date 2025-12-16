CREATE OR REPLACE FUNCTION add_partitions(table_name text, start_date DATE, end_date DATE)
RETURNS void AS $$
BEGIN
   WHILE start_date < end_date LOOP
      EXECUTE format('ALTER TABLE %I ADD PARTITION START (%L) END (%L) EXCLUSIVE', table_name, start_date, start_date + INTERVAL '1 month');
      start_date := start_date + INTERVAL '1 month';
   END LOOP;
END;
$$ LANGUAGE plpgsql;
