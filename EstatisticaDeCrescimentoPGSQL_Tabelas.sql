
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
