 CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '3'}  AND durable_writes = true;

 CREATE FUNCTION test.fLog (input double)
    CALLED ON NULL INPUT
    RETURNS double LANGUAGE java AS
    'return Double.valueOf(Math.log(input.doubleValue()));';