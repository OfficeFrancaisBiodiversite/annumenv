
-- Preparation des données
ALTER TABLE varnum_senges.rpg35_56_2018_v
  ALTER COLUMN geom TYPE geometry(geometry, 4326)
    USING ST_SetSRID(geom,4326);
	Update varnum_senges. rpg35_56_2018_v as t1 
	set geom =  ST_MakeValid(t1.geom) where ST_Isvalid(t1.geom) = 'False';
DROP INDEX IF EXISTS rpg35_56_2018_v_idx;
CREATE INDEX rpg35_56_2018_v_idx ON varnum_senges.rpg35_56_2018_v USING gist (geom);

ALTER TABLE varnum_senges.rpg35_56_2017_v
  ALTER COLUMN geom TYPE geometry(geometry, 4326)
    USING ST_SetSRID(geom,4326);
	Update varnum_senges. rpg35_56_2017_v as t1 
	set geom =  ST_MakeValid(t1.geom) where ST_Isvalid(t1.geom) = 'False';
DROP INDEX IF EXISTS rpg35_56_2017_v_idx ;
CREATE INDEX rpg35_56_2017_v_idx ON varnum_senges.rpg35_56_2017_v USING gist (geom);

--Preparation de l'environnement
Drop table IF EXISTS varnum_senges. test;
Drop table IF EXISTS varnum_senges. atlas;
Drop table IF EXISTS varnum_senges. min_env ;
Drop table IF EXISTS varnum_senges. ann_inter ;

DROP FUNCTION if EXISTS  varnum_senges.u_overlap_subb (bigint, geometry, character varying (25) );
DROP FUNCTION if EXISTS  varnum_senges.u_overlap_sub(bigint, geometry);
DROP FUNCTION if EXISTS  varnum_senges.u_overlap(geometry,bigint);
DROP FUNCTION if EXISTS  varnum_senges.first_step;
DROP FUNCTION if EXISTS  varnum_senges.min_env;
DROP FUNCTION if EXISTS  varnum_senges.min_envt2;
DROP FUNCTION if EXISTS  varnum_senges.ann_inter ;
DROP FUNCTION if EXISTS  varnum_senges.Exception_Union ;
DROP FUNCTION if EXISTS  varnum_senges.Exception_Intersection ;


-- select * from varnum_senges.exec(
-- 	'SELECT fid, code_cultu from varnum_senges.rpg'
-- 						  ||53::varchar(2)||'_'||2017::varchar(4)||' limit 100') ;


DROP TYPE IF EXISTS varnum_senges.u_overlap_sub_typeb CASCADE ;
DROP TYPE IF EXISTS varnum_senges.u_overlap_sub_type CASCADE ;
DROP TYPE IF EXISTS varnum_senges.u_overlap_type CASCADE ;

      CREATE TABLE varnum_senges. test(
      	id serial PRIMARY KEY,
      	geom geometry,
      	geomtype character varying(50),
      	step character varying(50),
      	fid1 bigint,
      	fid2 bigint,
      	crossed bigint
      );

CREATE FUNCTION varnum_senges.Exception_Union(geometry, geometry) RETURNS geometry
      AS
      $$
      Begin
      	Return ST_Union ($1,$2);
      	EXCEPTION
      	   			when sqlstate 'XX000' then
      				return $1;
      	end;
      $$
      LANGUAGE PLPGSQL
      ;
CREATE FUNCTION varnum_senges.Exception_Intersection(geometry, geometry) RETURNS geometry
      AS
      $$
      Begin
      	Return ST_Intersection ($1,$2);
      	EXCEPTION
      	   			when sqlstate 'XX000' then
      				return $1;
      	end;
      $$
      LANGUAGE PLPGSQL
      ;

CREATE TYPE varnum_senges.u_overlap_sub_type as (fid bigint, geom geometry);
	CREATE FUNCTION varnum_senges.u_overlap_sub(bigint,geometry) RETURNS varnum_senges.u_overlap_sub_type as
		$$ SELECT $1, $2	 $$
		LANGUAGE SQL ;

CREATE TYPE varnum_senges.u_overlap_type as (
                       fid bigint,
											 p_geom geometry,
										   it_geom varnum_senges.u_overlap_sub_type[]
											);


	CREATE FUNCTION varnum_senges.u_overlap(geometry, bigint /*fid*/)
                  RETURNS varnum_senges.u_overlap_type as
	$$
    SELECT $2, $1, array_agg(f.inter)
    FROM  (SELECT c2.geom ,  varnum_senges.u_overlap_sub(c2.fid1, 
				varnum_senges.Exception_Intersection($1, c2.geom))as inter
        FROM varnum_senges.rpg35_56_2017_v  as c2
        where ST_Overlaps($1, c2.geom) /* and ST_area(st_intersection ($1, c2.geom)) > 1*/
-- 							    and (ST_GeometryType(st_intersection ($1, c2.geom)) = 'ST_Polygon'
-- 									or ST_GeometryType(st_intersection ($1, c2.geom)) = 'ST_MultiPolygon')
       ) as f;
       ;
	$$
	LANGUAGE SQL ;

-- Vérification des "overlaps" : opération la plus longue à éviter en ensembliste
-- Select varnum_senges.u_overlap(geom) from varnum_senges.rpg35_56_2018_v ORDER BY surf_parc desc LIMIT 1000 ;

--____Début traitement_____

-- construction de l'atlas comportant les informations pour recréations des enveloppes mnimales et intersections
CREATE TABLE varnum_senges. atlas(
	id serial PRIMARY KEY,
	dic varnum_senges.u_overlap_type
);

-- repérage et enregistrement des intersections
	insert into varnum_senges. atlas (dic) (
		select (varnum_senges.u_overlap(geom, fid1)) FROM (select geom, fid1 from varnum_senges.rpg35_56_2018_v /* LIMIT 1000 */) a1
	-- 	select (varnum_senges.u_overlap(geom, fid)).* FROM (select geom, fid from varnum_senges.rpg35_56_2018_v LIMIT 10) a1     --! Syntaxe de séparation des colones
		);

    -- Select (dic).fid from  varnum_senges.atlas;   -- syntaxe d'appel des colones de type composite
  	Delete from  varnum_senges.atlas  where (dic).it_geom is null  ;  --supresssion des entrées sans intersections

	-- Nettoyage, indexation d'atlas et copie dans un atlas_b ayant les entités de la couche plus ancienne comme entrée
	DROP INDEX IF EXISTS atlas_fid_idx;
	CREATE INDEX atlas_fid_idx ON varnum_senges.atlas USING btree(id) WITH (fillfactor = 100);
	drop table if exists varnum_senges. atlas_b; -- atlas auxiliaire contenant les entités intersectantes
	-- de la couche la plus ancienne. Utile car la fonction unnest() qui pêrmet de l'extraire ne peut être appelée dans des clauses WHERE
	Create table varnum_senges. atlas_b  as
	(select (unnest((dic).it_geom)).* from varnum_senges.atlas order by (unnest((dic).it_geom)).fid asc);
	DROP INDEX IF EXISTS atlas_b_fid_idx;
	DROP INDEX IF EXISTS atlas_b_idx;
	CREATE INDEX atlas_b_fid_idx ON varnum_senges.atlas_b USING btree(fid) WITH (fillfactor = 100);
	CREATE INDEX  atlas_b_idx ON varnum_senges.atlas_b USING gist (geom);

    --report des cas non problématiques
    insert into varnum_senges.test (geom, geomtype, step, fid1 ) (
    	Select c1.geom, ST_GeometryType(c1.geom), 'Report' , c1.fid1
    	from varnum_senges.rpg35_56_2018_v as c1
    	/*where c1.fid1 not in (
    		select (dic).fid from varnum_senges.atlas )
    	*/
			WHERE   NOT EXISTS
				(
			SELECT  NULL
			FROM    varnum_senges.atlas t1
			WHERE   (t1.dic).fid = c1.fid1
				)
		)
    ;

        insert into varnum_senges.test (geom, geomtype, step, fid2 ) (
        	Select c1.geom, ST_GeometryType(c1.geom), 'Report' , c1.fid1
        	from varnum_senges.rpg35_56_2017_v as c1
        	/*where c1.fid1 not in (
        		select distinct fid from varnum_senges.atlas_b )
				--(unnest((dic).it_geom)).fid from varnum_senges.atlas ) */
				WHERE   NOT EXISTS
				(
			SELECT  NULL
			FROM    varnum_senges.atlas_b t1
			WHERE   t1.fid = c1.fid1
				)
        	)
        ;



-- construction des enveloppes minimales :
CREATE TABLE varnum_senges. min_env (id serial PRIMARY KEY, fid_t1 bigint, fid_t2 bigint, geom geometry);
	CREATE FUNCTION varnum_senges.min_env(varnum_senges.u_overlap_type) RETURNS  varnum_senges.u_overlap_sub_type as
		$body$
			DECLARE
			geo_construct geometry;
			compte int;
-- 			rec record ;
			rec  varnum_senges.u_overlap_sub_type ;
			Begin
				geo_construct = ($1).p_geom ;
				-- on enregistre le nombre d'entrée dans les intersections pour traitement différencié
				compte = ( select json_array_length(
						JSON_AGG (ROW_TO_JSON(table_a))
						) FROM (select unnest(($1).it_geom)) table_a  );
	-- 					raise notice 'ite % loop1 à %', (t_row).fid, compte;
							if compte = 1
								then geo_construct = ST_Difference (geo_construct,  (unnest(($1).it_geom)).geom ) ;
								else
		-- 							raise notice 'cas compliqué %', (select (unnest((t_row).it_geom)).fid limit 1);
									for rec in select (unnest(($1).it_geom)).*
									loop
		-- 								raise notice 'ite % loop2', rec.fid;
										geo_construct = ST_Difference (geo_construct,  ST_Buffer(rec.geom, -0.001));
									end loop ;
							end if;
-- 			Select ($1).fid, geo_construct  Into rec;
-- 			return rec;
			 return varnum_senges.u_overlap_sub(($1).fid, geo_construct );
			end;
		$body$
		LANGUAGE plpgSQL;

    CREATE FUNCTION varnum_senges.min_env_t2(bigint) RETURNS varnum_senges.u_overlap_sub_type as
      $body$
        DECLARE
        geo_construct geometry;
        geo_diff geometry;
        compte int;
        rec  varnum_senges.u_overlap_sub_type ;
        Begin
          geo_construct = (select geom from varnum_senges.rpg35_56_2017_v where fid1 = $1 limit 1) ;
          compte = (select count (*) from (					-- on enregistre le nombre d'entrée dans les intersections pour traitement différencié
                  select fid from varnum_senges.atlas_b where fid = $1)a1 );
      -- 					raise notice 'ite % loop1 à %', (t_row).fid, compte;
                if compte = 1
                  then  geo_diff = ( select geom from varnum_senges.atlas_b where fid = $1 );
                        geo_construct = ( select ST_Difference (geo_construct,  geo_diff) );
                  else
      -- 							raise notice 'cas compliqué %', (select (unnest((t_row).it_geom)).fid limit 1);
                    for rec in select * from varnum_senges.atlas_b where fid = $1
                    loop
      -- 								raise notice 'ite % loop2', rec.fid;
                      geo_construct = ST_Difference (geo_construct,  ST_Buffer(rec.geom, -0.001));
                    end loop ;
                end if;
  -- 			Select ($1).fid, geo_construct  Into rec;
  -- 			return rec;
         return varnum_senges.u_overlap_sub($1, geo_construct );
        end;
      $body$
      LANGUAGE plpgSQL;



insert into varnum_senges. min_env (fid_t1, geom) (
	select (varnum_senges.min_env(dic)).*  from varnum_senges.atlas    /* aller sur t1 */
) ;


insert into varnum_senges. min_env (fid_t2, geom) (
	select distinct (varnum_senges.min_env_t2(fid) ).*  from varnum_senges.atlas_b  /* retour sur t2 */
) ;

delete from  varnum_senges. min_env
  where  geom is null or ST_GeometryType(geom) not in ('ST_MultiPolygon', 'ST_Polygon')
  or ST_AREA(geom) < 25  ;

DROP INDEX IF EXISTS min_env_idx;
DROP INDEX IF EXISTS min_env_fid_idx ;

CREATE INDEX min_env_fid_idx ON varnum_senges.min_env USING btree(id) WITH (fillfactor = 100);
CREATE INDEX  min_env_idx ON varnum_senges.min_env USING gist (geom);
-- Arbitrage des fusions des intersections : Minenv full mandatory !!!
	Drop table IF EXISTS varnum_senges. ann_inter ;
	DROP FUNCTION if EXISTS  varnum_senges.ann_inter ;
	CREATE TABLE varnum_senges. ann_inter (id serial PRIMARY KEY, step character varying (20), fid1 bigint, fid2 bigint, choix varchar(6), geom geometry);

	CREATE FUNCTION varnum_senges.ann_inter(bigint, geometry, bigint, geometry, geometry) RETURNS varnum_senges.u_overlap_sub_type as
		$body$
		declare
			parent geometry;
			tampon geometry;

		begin
				if ST_area($5) < 75
					or ST_Geometrytype($5) not in ('ST_Polygon', 'ST_Multipolygon')
	-- 				or ( ST_Geometrytype(ST_Intersection (ST_ExteriorRing($5), $2)) = 'ST_Point' and ST_Geometrytype(ST_Intersection (ST_ExteriorRing($5), $4)) = 'ST_Point' )
					or ST_intersects($2, $5) = 'False'
					or ST_intersects($4, $5) = 'False'
				then
					tampon = ST_MakePolygon( ST_GeomFromText('LINESTRING(0 0, 0 0, 0 0, 0 0)'));  -- polygon null : 0103000000010000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
					insert into varnum_senges. ann_inter (step, fid1, fid2, choix)
									(Select 'Insuff_area', $1,$3, 'neant') ;
					return ($1, tampon);  -- retour pour controle
				else
          if ST_AREA($5) > 150   -- cas ou l'on maintient l'intersection car suffisante par la taille
          then tampon = $5 ;
          insert into varnum_senges. ann_inter (step, fid1, fid2, choix, geom)
                  (Select 'Suff_area', $1,$3, 'neant', $5) ;
				  return ($1, tampon);
          else
  		-- 				ST_CollectionExtract(ST_Difference( $2, $4),2)   --extraction des lignes
  					if ST_Length(	ST_Intersection (ST_ExteriorRing($5), $2) ) > ST_Length( ST_Intersection (ST_ExteriorRing($5), $4) )
  					then
  						if $1 in (select distinct fid1 from varnum_senges. ann_inter where choix = 'new')
    						then
    							parent = (select distinct geom from varnum_senges. ann_inter
										  where fid1 = $1 and step in ('Recursiv', 'No_Pbm', 'Suff_area') limit 1 ) ;
    							tampon = ST_MakePolygon(ST_ExteriorRing(varnum_senges.Exception_Union( parent ,$5))) ;
    							delete from varnum_senges. ann_inter where fid1 = $1 and choix = 'new';
    							insert into varnum_senges. ann_inter (step, fid1, fid2, choix, geom)
    								(Select 'Recursiv', $1,$3, 'new',tampon ) ;

    						else
    							tampon = ST_MakePolygon(ST_ExteriorRing(varnum_senges.Exception_Union( $2 ,$5))) ;
    							insert into varnum_senges. ann_inter (step, fid1, fid2, choix, geom)
    								(Select 'No_pbm', $1,$3, 'new', tampon) ;

    						end if;

    						Return ($1, tampon )  ;
    							-- Raise Notice 'choix a %', $1;
    				else
    						if $3 in (select distinct fid2 from varnum_senges. ann_inter where choix = 'old')
    						then
    							parent = (select distinct  geom from varnum_senges. ann_inter
										  where fid2 = $3 and step in ('Recursiv', 'No_Pbm', 'Suff_area') limit 1 ) ;
    							tampon = ST_MakePolygon(ST_ExteriorRing(varnum_senges.Exception_Union( parent ,$5))) ;
    							delete from varnum_senges. ann_inter where fid2 = $3 and choix = 'old';
    							insert into varnum_senges. ann_inter (step, fid1, fid2, choix, geom)
    								(select 'Recursiv', $1,$3, 'old',tampon ) ;
    						else
    							tampon = ST_MakePolygon(ST_ExteriorRing(varnum_senges.Exception_Union( $4,$5))) ;
    							insert into varnum_senges. ann_inter (step, fid1, fid2, choix, geom)
    								(select 'No_pbm', $1,$3, 'old', tampon) ;  --!!
    						end if;

  						Return ($3, tampon) ;
  							-- Raise Notice 'choix b %', $3;
    				end if;
  				end if;
        end if;

		 end;
		 $body$
		LANGUAGE plpgSQL ;

	DO
		$BODY$

		Declare
			o_row   varnum_senges. atlas%rowtype;
			t_row   varnum_senges.u_overlap_type ;
			reponse varnum_senges.u_overlap_sub_type;
			rec varnum_senges.u_overlap_sub_type;
			compte int;
			var0 int;
			var1 bigint;   -- fid1 new
			var2 geometry;	   -- min_env de fid1
			var3 bigint;   -- fid2 old
			var4 geometry;	   -- min_env de fid2
			var5 geometry;     -- intersectionS

		BEGIN
			For o_row in select * from varnum_senges.atlas  --partie 1 : construction de l'annuaire de rattachement des intersections
			loop
				var0 = o_row.id;
				t_row = o_row.dic;
				var1 = (t_row).fid ;
				var2 = (Select geom from varnum_senges. min_env where fid_t1= (t_row).fid limit 1);

				compte = ( select json_array_length(
						JSON_AGG (ROW_TO_JSON(table_a))
						) FROM (select unnest((t_row).it_geom)) table_a  );
-- 							raise notice 'ite % loop1 à %', (t_row).fid, compte;
					if compte = 1
					then
						var3 = (select (unnest((dic).it_geom)).fid from varnum_senges.atlas where id = var0 );
						var4 = (Select geom from varnum_senges. min_env where fid_t2 = var3 );
-- 						if ST_Geometrytype(var4) not in('ST_Polygon', 'ST_MultiPolygon') then raise notice 'pas de min env correspondante %', var1;
-- 						end if;
						var5 = (select (unnest((dic).it_geom)).geom from varnum_senges.atlas where id = var0 );

						reponse = ( select
							varnum_senges.ann_inter(var1, var2, var3, var4, var5) );
						-- raise notice 'cas simple  : %',reponse;

					else
						-- raise notice 'cas compliqué % pour %', (select (t_row).fid ), compte;
						for rec in (select (unnest((t_row).it_geom)).* )
						loop
		-- 					raise notice 'ite % loop2', rec.fid;
							var3 = rec.fid ;
							var4 = (Select geom from varnum_senges. min_env where fid_t2 = var3 );
							var5 = rec.geom;
							reponse = ( select
								varnum_senges.ann_inter(var1, var2, var3, var4, var5) );
						end loop ;
					end if;
			end loop;
      end ;
      $BODY$;
      --partie 2 : ajout des cas pour lesquels les min_env associées < 150

Update  varnum_senges. ann_inter
Set step = 'Insuff_area'
where step != 'Insuff_area' and ST_SRID(geom) is null ;


	DO
	$$
  	Declare
      t_row   varnum_senges.ann_inter %ROWTYPE;
      fidT1 bigint;
      fidT2 bigint;
      geom_t1 geometry ;
      geom_t2 geometry ;
      tampon geometry;
  	BEGIN
      For t_row in select * from varnum_senges.ann_inter where step = 'Suff_area' order by id asc limit 70000
      loop
        fidT1 =  t_row.fid1;
        fidT2 =  t_row.fid2;
        -- repérage des min_env < 150  déjà aggrégées
        if EXISTS
				(
			SELECT  NULL
			FROM    varnum_senges.ann_inter
			WHERE   fid1 = fidT1
					and step in ('P2-t1-2_agg','P2-t1_agg')
				)
-- 		Alternative non-optimisée :
--         if  ( select count (*) from (					-- cas ou min_env1 est déjà aggrégé
-- 								select * from varnum_senges.ann_inter where step in ('P2-t1-2_agg','P2-t1_agg') and fid1 = fidT1  )a1 )> 0

		then
           if EXISTS
				(
			SELECT  NULL
			FROM    varnum_senges.ann_inter
			WHERE   fid2 = fidT2
					and step in ('P2-t1-2_agg','P2-t2_agg')
				)

			  then
				geom_t1 = ST_MakePolygon( ST_GeomFromText('LINESTRING(0 0, 0 0, 0 0, 0 0)'));
				geom_t2 = ST_MakePolygon( ST_GeomFromText('LINESTRING(0 0, 0 0, 0 0, 0 0)'));
          else
            geom_t1 = ST_MakePolygon( ST_GeomFromText('LINESTRING(0 0, 0 0, 0 0, 0 0)'));
            geom_t2 = (select geom from varnum_senges.min_env where fid_t2 = fidT2);
          end if;
        else
          if EXISTS
				(
			SELECT  NULL
			FROM    varnum_senges.ann_inter
			WHERE   fid2 = fidT2
					and step in ('P2-t1-2_agg','P2-t2_agg')
				)
		  then
            geom_t1 = (select geom from varnum_senges.min_env where fid_t1 = fidT1);
            geom_t2 = ST_MakePolygon( ST_GeomFromText('LINESTRING(0 0, 0 0, 0 0, 0 0)'));
          else    -- cas où les deux min_env sont libres
            geom_t1 = (select geom from varnum_senges.min_env where fid_t1 = fidT1);
            geom_t2 = (select geom from varnum_senges.min_env where fid_t2 = fidT2);
          end if;
        end if;
        --traitement conditionnel
      if ST_AREA(geom_t1)  > 150 and St_Area(geom_t1)/ST_PERIMETER(geom_t1) > 0.01
      then
          if ST_AREA(geom_t2)  > 150 and St_Area(geom_t2)/ST_PERIMETER(geom_t2) > 0.01
		  -- cas de conservation des trois
          then
			insert into varnum_senges. ann_inter (step, fid1, geom)
            (select 'P2-t1_keep', fidT1, geom_t1 ) ;
            insert into varnum_senges. ann_inter (step, fid2, geom)
            (select 'P2-t2_keep', fidT2, geom_t2 ) ;
			insert into varnum_senges. ann_inter (step, fid1, fid2, geom)
            (select 'P2-inter_keep', fidT1, fidT2, t_row.geom) ;
			delete from varnum_senges. ann_inter where fid1 = fidT1 and fid2 = fidT2 and step='Suff_area';
-- 				raise notice 'triple conservation, %, %', fidT1, fidT2;


          else          -- cas de conservation d'inter et new
			  tampon = (varnum_senges.Exception_Union(t_row.geom , geom_t2 )) ;
              if ST_SRID(geom_t2) = 0 or St_Area(geom_t2)/ST_PERIMETER(geom_t2) < 0.01
              then tampon = t_row.geom;
			  end if ;
-- 			      raise notice 'geom t2 : % , %' , fidT2, ST_SRID(geom_t2);
            insert into varnum_senges. ann_inter (step, fid1, geom)
            (select 'P2-t1_keep', fidT1, geom_t1 ) ;
            insert into varnum_senges. ann_inter (step, fid1, fid2, geom)
            (select 'P2-t2_agg', fidT1, fidT2, tampon) ;
            delete from varnum_senges. ann_inter where fid1 = fidT1 and fid2 = fidT2 and step='Suff_area';
          end if;
        else
          if ST_AREA(geom_t2)  > 150 and St_Area(geom_t2)/ST_PERIMETER(geom_t2) > 0.01
		  -- cas de conservation d'inter et old
          then
              if ST_SRID(geom_t1) = 0 or St_Area(geom_t1)/ST_PERIMETER(geom_t1) < 0.01
              then tampon = t_row.geom;
              else tampon = (varnum_senges.Exception_Union(t_row.geom , geom_t1 )) ;
              end if ;
-- 			        raise notice 'geom t1 : % , %' , fidT1, ST_SRID(geom_t1);
            insert into varnum_senges. ann_inter (step, fid1, fid2, geom)
            (select 'P2-t1_agg', fidT1, fidT2, tampon) ;
            insert into varnum_senges. ann_inter (step, fid2, geom)
            (select 'P2-t2_keep', fidT2, geom_t2 ) ;
            delete from varnum_senges. ann_inter where fid1 = fidT1 and fid2 = fidT2 and step='Suff_area';
          else   --cas de conservation de l'inter seul
              if ST_SRID(geom_t1) = 0 or St_Area(geom_t1)/ST_PERIMETER(geom_t1) < 0.01
              then tampon = t_row.geom;
              else tampon = (varnum_senges.Exception_Union(t_row.geom , geom_t1 )) ;
              end if ;
              if ST_SRID(geom_t2) = 0
              then tampon = tampon ;
              else tampon = (varnum_senges.Exception_Union(tampon, geom_t2 )) ;
              end if ;
-- 			      raise notice 'geom dbl : % , %, %, %' ,fidT1, ST_SRID(geom_t1), fidT2, ST_SRID(geom_t2);
            insert into varnum_senges. ann_inter (step, fid1, fid2, geom)
            (select 'P2-t1-2_agg', fidT1, fidT2, tampon) ;
            delete from varnum_senges. ann_inter where fid1 = fidT1 and fid2 = fidT2 and step='Suff_area';
          end if;
        end if;
      end loop;
	end ;
  $$


  -- troisième et dernière partie : traitement des erreurs/doublons
  	DO
	$BODY$
  	Declare
      t_row   varnum_senges.ann_inter %ROWTYPE;
	  x_row   varnum_senges.ann_inter %ROWTYPE;
      fidT1 bigint;
      fidT2 bigint;
      geom_t1 geometry ;
      geom_t2 geometry ;
      tampon geometry;
  	BEGIN
      For t_row in select * from varnum_senges.ann_inter where step in  ('Recursiv', 'No_Pbm')
      loop
        fidT1 =  t_row.fid1;
        fidT2 =  t_row.fid2;

        if EXISTS
				(
			SELECT  NULL
			FROM    varnum_senges.ann_inter
			WHERE   fid1 = fidT1 and fid1 = fidT1
					and step in ('P2-t1-2_agg','P2-t2_agg','P2-t1_agg','P2-inter_keep' )
				)

		then
			For x_row in select * from varnum_senges.ann_inter where step in ('P2-t1-2_agg','P2-t2_agg','P2-t1_agg','P2-inter_keep' )
			and fid1 = fidT1 and fid1 = fidT1
			loop
				if ST_Contains(x_row.geom, t_row.geom)
				then
					delete from varnum_senges. ann_inter where fid1 = fidT1 and fid2 = fidT2 and step= t_row.step;
        		end if;
			end loop;

        end if;
      end loop;
    end ;
  $BODY$;

DROP INDEX IF EXISTS ann_inter_idx;
DROP INDEX IF EXISTS ann_inter_fid_idx ;

CREATE INDEX  ann_inter_fid_idx ON varnum_senges. ann_inter USING btree(choix) WITH (fillfactor = 100);
CREATE INDEX ann_inter_idx ON varnum_senges. ann_inter USING gist (geom);

-- Reconstruction de la couche traitée
insert into varnum_senges.test (geom, geomtype, step, fid1, fid2 ) (
	Select c1.geom, ST_GeometryType(c1.geom), 'reconstruit' , c1.fid1, c1.fid2
	from varnum_senges.ann_inter as c1
	where c1.geom is not null
	)
;

insert into varnum_senges.test (geom, geomtype, step, fid1 ) (
	WITH dump AS (
		SELECT  (ST_DUMP(sub.geom)).geom AS geometry, ST_GeometryType((ST_DUMP(sub.geom)).geom) As geomtype, sub.step as step, sub.fid1 as fid1        --columns from your multipolygon table
		FROM varnum_senges. test as sub where ST_Geometrytype(geom) = 'ST_MultiPolygon'
	)

	SELECT  geometry::geometry(Polygon,4326), geomtype, step, fid1    --type cast using SRID from multipolygon
	  FROM dump
	);

delete from varnum_senges.test where ST_Geometrytype(geom) = 'ST_MultiPolygon'  ;
-- spe TxT : pas de polygon de moins de 10
delete from varnum_senges.test where ST_AREA(geom)<10 ;

-- probleme de MultiLine String
-- 	select geom, geomtype, step, fid1 from varnum_senges. test where fid1 in
-- 	(SELECT /* (ST_DUMP(sub.geom)).geom AS geometry, ST_GeometryType((ST_DUMP(sub.geom)).geom) As geomtype, sub.step as step, */sub.fid1 as fid1        --columns from your multipolygon table
-- 		FROM varnum_senges. test as sub where ST_Geometrytype(geom) = 'ST_MultiLineString'       )

DROP INDEX IF EXISTS test_fid_idx;
DROP INDEX IF EXISTS test_idx ;

CREATE INDEX test_fid_idx ON varnum_senges.test USING btree(fid1) WITH (fillfactor = 100);
CREATE INDEX test_idx ON varnum_senges.test USING gist (geom);
ALTER TABLE varnum_senges.test
  ALTER COLUMN geom TYPE geometry(POLYGON, 2154)
    USING ST_SetSRID(geom,2154);

--Gestion des dernieres approximations
ALTER TABLE varnum_senges. test ALTER COLUMN geom SET DATA TYPE geometry;

UPDate varnum_senges. test
set geom = st_buffer(geom, -0.001);

ALTER TABLE varnum_senges. test ALTER COLUMN geom
SET DATA TYPE geometry(MultiPolygon) USING ST_Multi(geom);
