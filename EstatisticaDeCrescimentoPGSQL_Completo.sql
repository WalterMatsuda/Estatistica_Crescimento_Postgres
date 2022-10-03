
  CREATE TYPE estatistica_composto AS(
  table_schema VARCHAR(63) , 
  table_name VARCHAR(63) ,
  size BIGINT 
    );  

  CREATE EXTENSION IF NOT EXISTS dblink;



  CREATE SEQUENCE IF NOT EXISTS public.estatistica_hosts_seq ; 
  CREATE TABLE  IF NOT EXISTS public.estatistica_hosts(
  id BIGINT DEFAULT nextval('estatistica_hosts_seq'::regclass) ,
  host VARCHAR(100) NOT NULL,
  dbname VARCHAR(100) NOT NULL, 
  dbuser VARCHAR(100) NOT NULL, 
  dbpassword VARCHAR(100) NOT NULL, 
  alias VARCHAR(100) , 
  descricao VARCHAR(300),
  port INT NOT NULL DEFAULT 5432, 
  total_estatisticas INT DEFAULT 0 ,
  size BIGINT , 
  ativo BOOLEAN DEFAULT false,
  ultimo_relatorio TIMESTAMP ,
  qtd_relatorios INT DEFAULT 0 ,
  CONSTRAINT estatistica_hosts_pkey PRIMARY KEY(id) 


  );


  CREATE SEQUENCE IF NOT EXISTS public.estatistica_crescimento_seq; 
  CREATE TABLE IF NOT EXISTS public.estatistica_crescimento(
  id BIGINT DEFAULT nextval('estatistica_crescimento_seq'::regclass ) ,
  data_relatorio TIMESTAMP , 
  comp_estatistica_array estatistica_composto[] NOT NULL, 
  dbsize BIGINT ,  
  estatistica_hostsid BIGINT NOT NULL , 
  CONSTRAINT fk_estatistica_hosts FOREIGN KEY(estatistica_hostsid) REFERENCES estatistica_hosts(id) ON DELETE NO ACTION ON UPDATE NO ACTION NOT DEFERRABLE ,
  CONSTRAINT estatistica_crescimento_pkey PRIMARY KEY (id)    
  );

CREATE OR REPLACE FUNCTION public.estatistica_crescimento (
)
RETURNS varchar AS
$body$
  DECLARE 
  rHosts RECORD ;
  rInformationSchema  RECORD ; 
  tConexao TEXT ; 
  biRowCount BIGINT ; 
  iCount BIGINT ; 
  tSize TEXT ; 
  biDbSize BIGINT ;
  rConSucesso RECORD ;
  tBigQuery TEXT :=''; 
  iExecutionCount INT :=0; 
  rBigQuery RECORD ; 

  BEGIN 



  CREATE TEMPORARY TABLE tmp_table (
  table_name VARCHAR(200) , 
  table_schema VARCHAR(100) , 
  table_size BIGINT 
  )ON COMMIT DROP ;

  SELECT COUNT(id) INTO iCount FROM public.estatistica_hosts;
  IF iCount = 0 THEN 
  RAISE EXCEPTION 'A tabela de hosts está vaiza'
        USING HINT = 'Adicione os hosts que serão monitorados na tabela public.estatistica_hosts';

  END IF ; 

  FOR rHosts IN SELECT host , dbname , dbuser , dbpassword , alias , port , id FROM public.estatistica_hosts WHERE ativo


  LOOP

  tConexao := 'dbname='||rHosts.dbname||' hostaddr='||rHosts.host||
    ' user='||rHosts.dbuser||' password='||rHosts.dbpassword||' port='||rHosts.port;
   

  tBigQuery ='';
  iExecutionCount=0;

  FOR  rInformationSchema IN  SELECT link.table_schema , link.table_name , link.key_column FROM dblink(tConexao , $$SELECT kcu.table_schema,
         kcu.table_name,
         kcu.column_name as key_column 
  FROM information_schema.table_constraints tco
  INNER JOIN information_schema.key_column_usage kcu on kcu.constraint_name = tco.constraint_name and kcu.constraint_schema = tco.constraint_schema and kcu.constraint_name = tco.constraint_name
  WHERE tco.constraint_type = 'PRIMARY KEY'
  ORDER BY kcu.table_schema,
           kcu.table_name$$ ) AS link (table_schema text , table_name text , key_column text) 

  LOOP

  biRowCount := 0 ;  

  IF iExecutionCount =0 THEN 
  iExecutionCount :=1 ; 
  ELSE
  tBigQuery := tBigQuery||' UNION ';
  iExecutionCount := iExecutionCount+1;
  END IF ; 

  tBigQuery := tBigQuery || ' SELECT '||E'\''||rInformationSchema.table_name||E'\''||' AS table_name ,'
  ||E'\''|| rInformationSchema.table_schema ||E'\''
  ||',pg_total_relation_size('||E'\''||rInformationSchema.table_schema||'.'||rInformationSchema.table_name||E'\''||') '  
   ||'   AS table_size  ';

  END LOOP ; 


  FOR rBigQuery IN SELECT bq.table_name , bq.table_schema , bq.table_size FROM dblink(tConexao , tBigQuery) AS bq(table_name TEXT , table_schema TEXT ,table_size BIGINT )
  LOOP 

  INSERT INTO tmp_table (table_name , table_schema ,table_size  )VALUES ( rBigQuery.table_name , rBigQuery.table_schema  ,rBigQuery.table_size );

  END LOOP ; 



  SELECT dbs.dbSize INTO biDbSize FROM dblink(tConexao , 'SELECT pg_database_size('||$$'$$||rHosts.dbname||$$'$$||') AS dbSize' ) AS dbs(dbSize  BIGINT);

  INSERT INTO public.estatistica_crescimento (data_relatorio, estatistica_hostsid , comp_estatistica_array ,dbsize   )
  SELECT now() , rHosts.id  , array_agg(array[row( a.table_schema ,a.table_name ,  a.table_size)::estatistica_composto] )  , biDbSize
  FROM tmp_table AS a;
  DELETE FROM tmp_table;

  UPDATE public.estatistica_hosts 
  SET size = biDbSize , ultimo_relatorio = now() , qtd_relatorios = b.count 
  FROM (SELECT COUNT(id) AS count FROM estatistica_crescimento  WHERE rHosts.id = estatistica_hostsid )AS b
  WHERE id = rHosts.id;
  END LOOP ; 




  RETURN 'ok';
  END
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100;

ALTER FUNCTION public.estatistica_crescimento ()
  OWNER TO postgres;



CREATE OR REPLACE FUNCTION public.estatistica_crescimentotable (
  pdatainicio timestamp,
  pdatafim timestamp
)
RETURNS text AS
$body$
DECLARE

rEstatatisticaDatas RECORD ; 
rEstatatisticaHosts RECORD ; 
iExecutionCount INTEGER ; 
tQuery TEXT := '' ; 
tTableName TEXT ; 
bComparer BOOLEAN ; 
BEGIN 
--Criação da tabela para armazenar o relatório 
tTableName := $$"$$||'relcrescimento_'||to_char(pdatainicio ,'DDMMYYYY/HH:mi' )||'_'||to_char(pdatafim ,'DDMMYYYY/HH:mi' )||'_'||to_char(now() ,'DDMMYYYY/HH:mi' )||$$"$$;
SELECT INTO tQuery 'CREATE TABLE '||tTableName||'( estatistica_hostsid BIGINT  ,table_schema VARCHAR(63) ,table_name VARCHAR(63) ,';

FOR rEstatatisticaDatas IN SELECT data_relatorio FROM estatistica_crescimento WHERE data_relatorio >=pDataInicio AND data_relatorio<=pDataFim GROUP BY data_relatorio 
LOOP 

tQuery :=tQuery||$$"$$||rEstatatisticaDatas.data_relatorio||$$"$$||' BIGINT ,';

END LOOP ; 
tQuery := tQuery||' Media BIGINT , Total BIGINT )';
EXECUTE tQuery ;

FOR rEstatatisticaHosts IN SELECT estatistica_hostsid , MAX(dbsize) FROM estatistica_crescimento WHERE data_relatorio >=pDataInicio AND data_relatorio<= pDataFim  GROUP BY estatistica_hostsid,dbsize  ORDER BY dbsize DESC
LOOP 
iExecutionCount := 0 ; 
	

FOR rEstatatisticaDatas IN SELECT data_relatorio FROM estatistica_crescimento WHERE data_relatorio >=pDataInicio AND data_relatorio<=pDataFim GROUP BY data_relatorio 
LOOP 

	IF iExecutionCount =0  THEN 
	EXECUTE 'INSERT INTO '||tTableName||'(estatistica_hostsid ,table_schema, table_name ,'||$$"$$||rEstatatisticaDatas.data_relatorio||$$"$$||') 
	SELECT '||rEstatatisticaHosts.estatistica_hostsid||' , (a.comp_array).table_schema , (a.comp_array).table_name	, (a.comp_array).size 
	FROM (SELECT unnest( a.comp_estatistica_array ) AS comp_array FROM estatistica_crescimento AS a 
	WHERE a.estatistica_hostsid ='||rEstatatisticaHosts.estatistica_hostsid||' AND a.data_relatorio='||$$'$$||rEstatatisticaDatas.data_relatorio||$$'$$||') AS a 
	ORDER BY (a.comp_array).size DESC ';

	ELSE 
	EXECUTE 'UPDATE '||tTableName||' SET '||$$"$$||rEstatatisticaDatas.data_relatorio||$$"$$||
	'= sq.size 
	FROM (SELECT b.estatistica_hostsid AS host,  (b.comp_array).size AS size , (b.comp_array).table_schema AS table_schema, (b.comp_array).table_name AS table_name 
	FROM (SELECT unnest(comp_estatistica_array) AS comp_array, estatistica_hostsid FROM estatistica_crescimento WHERE estatistica_hostsid = '
	||rEstatatisticaHosts.estatistica_hostsid||' AND data_relatorio = '||$$'$$||rEstatatisticaDatas.data_relatorio||$$'$$||' )AS b )AS sq 
	WHERE '||tTableName||'.table_name = sq.table_name AND '||tTableName||'.table_schema=sq.table_schema AND estatistica_hostsid=sq.host';

	EXECUTE 'SELECT (SELECT COUNT(estatistica_hostsid) FROM '||tTableName||'WHERE estatistica_hostsid='||rEstatatisticaHosts.estatistica_hostsid||') <> (SELECT array_length(comp_estatistica_array ,1 ) FROM estatistica_crescimento WHERE estatistica_hostsid = '||rEstatatisticaHosts.estatistica_hostsid||' AND data_relatorio='||$$'$$||rEstatatisticaDatas.data_relatorio||$$'$$||'  )  ' INTO bComparer;

	IF bComparer THEN 

	EXECUTE 'INSERT INTO '||tTableName||'( estatistica_hostsid ,table_schema , table_name,'||$$"$$||rEstatatisticaDatas.data_relatorio||$$"$$||' )  
	SELECT  qprimary.hostid,qprimary.table_schema , qprimary.table_name , qprimary.size FROM 
	(SELECT '||rEstatatisticaHosts.estatistica_hostsid||' AS hostid , (a.comp_array).table_schema AS table_schema , (a.comp_array).table_name	AS table_name,(a.comp_array).size AS size
	FROM (SELECT unnest( a.comp_estatistica_array ) AS comp_array FROM estatistica_crescimento AS a WHERE a.estatistica_hostsid ='||rEstatatisticaHosts.estatistica_hostsid||' AND a.data_relatorio='||$$'$$||rEstatatisticaDatas.data_relatorio||$$'$$||') AS a	) AS qprimary
	INNER JOIN (
	SELECT (comq1.cea1).table_schema , (comq1.cea1).table_name FROM ( SELECT unnest(q2.comp_estatistica_array) AS cea1 FROM  estatistica_crescimento AS q2 WHERE q2.estatistica_hostsid = '||rEstatatisticaHosts.estatistica_hostsid||' AND q2.data_relatorio = '||$$'$$||rEstatatisticaDatas.data_relatorio||$$'$$||' ) AS comq1 
	EXCEPT ALL 
	SELECT comq2.table_schema , comq2.table_name FROM ( SELECT table_name , table_schema FROM '||tTableName||' AS q3 WHERE q3.estatistica_hostsid = '||rEstatatisticaHosts.estatistica_hostsid||'  ) AS comq2
	) AS qsecondary ON qprimary.table_schema = qsecondary.table_schema  AND qprimary.table_name = qsecondary.table_name
	';
	END IF ; 

	RETURN tTableName;



	END IF ;
	iExecutionCount := iExecutionCount+1;


END LOOP ;


END LOOP ;



END
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100;

ALTER FUNCTION public.estatistica_crescimentotable (pdatainicio timestamp, pdatafim timestamp)
  OWNER TO postgres;