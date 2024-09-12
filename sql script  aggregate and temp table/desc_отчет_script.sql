

-- агрегируем поля rep_beg, campaign_type, campaign_name_plan
drop table if exists  aggregate_campaign;
create temporary table aggregate_campaign
                               with (appendonly=true, compresstype=zstd, compresslevel=1)
                               on commit preserve rows
                               as 
      (select distinct 
      
	        date_trunc('month', campaign_start)::date as  rep_beg,
		    'НОВЫЙ ДОХОД (РАЗВИТИЕ/ДОКУПКИ)' as  campaign_type,
	        campaign_start, 
	        campaign_name,
	        case 
		        
		    when lower(campaign_name) like '%welcome%'
				then 'Welcome'
			when campaign_name like '%_сущ_%' 
				then
					substring(campaign_name, Position('_сущ_' in campaign_name)+5, length(campaign_name))
			when campaign_name like '%_потенц_%'
				then 
					substring(campaign_name, Position('_потенц_' in campaign_name)+8, length(campaign_name))
			else 'Welcome'
			
			end as campaign_name_plan
		
		from crm_b2b_sb.active_company 
		
) with data distributed randomly;
analyze aggregate_campaign;


-- добавлено поле product_fix, удалены записи, которые в поле vas
drop table if exists  product_final;
create temporary table product_final
                               with (appendonly=true, compresstype=zstd, compresslevel=1)
                               on commit preserve rows
                               as 
      (with t as (		
			SELECT product_prod, 
			       product_vas, 
			       CASE WHEN product_vas  in ('Защита от DoS/DDoS-атак', 
			' Защита от DoS/DDoS-атак (В2В)', 
			' Защита от DoS/DDoS-атак (Cloud)', 
			'WiFi для бизнеса', 
			'Фиксированная связь', 
			'ШПД', 
			'VPN', 
			'SD_WAN' )  THEN product_vas 
			      END AS product_fix 
			FROM crm_b2b_sb.prod_equal_vas 
			)
			
			select product_prod, case 
			when product_vas in ('Защита от DoS/DDoS-атак', 
			' Защита от DoS/DDoS-атак (В2В)', 
			' Защита от DoS/DDoS-атак (Cloud)', 
			'WiFi для бизнеса', 'Фиксированная связь', 
			'ШПД', 
			'VPN', 
			'SD_WAN' )  THEN null
			else product_vas
			end,
			product_fix from t

		  	  
) with data distributed randomly;
analyze product_final;


-- создаем из таблицы crm_b2b_sb.products_branch список продуктов target_prod и target_vas
drop table if exists  aggregate_product;
create temporary table aggregate_product
                               with (appendonly=true, compresstype=zstd, compresslevel=1)
                               on commit preserve rows
                               as 
      (
		with t as (select distinct pb.industry, string_agg(distinct pev.product_vas, ', ') as target_vas  from crm_b2b_sb.products_branch pb 
		left join product_final pev on pev.product_prod = pb.product 
		group by pb.industry),
		s as (select distinct pb.industry, string_agg(distinct pb.product, ', ') as target_prod  from crm_b2b_sb.products_branch pb 
		left join product_final pev on pev.product_prod = pb.product 
		group by pb.industry),
		g as (select distinct pb.industry, string_agg(distinct product_fix, ', ') as target_fix from crm_b2b_sb.products_branch pb 
		left join product_final pev on pev.product_prod = pb.product 
		group by pb.industry)
		select distinct a.industry, b.target_prod, a.target_vas, g.target_fix
		from t a 
		left join s b on a.industry=b.industry
		left join g on g.industry = a.industry
) with data distributed randomly;
analyze aggregate_product;


-- агрегируем поле type, target_prod, target_vas
drop table if exists  aggregate_target_prod;
create temporary table aggregate_target_prod
                               with (appendonly=true, compresstype=zstd, compresslevel=1)
                               on commit preserve rows
                               as 
      (select 
            t.*, 
			case
				when 
					t.campaign_name_plan in ('Продление опций (Абонемент фикс)',
										'Дожим воронки',
										'Welcome',
										'Отраслевая Бизнес-диагностика',
										'Удвоение пакетов',
										'Допродажа клиентам из ЛК МТС Бизнес',
										'Моно-клиенты Mob_Fix',
										'Моно-клиенты_Mob') 
				then 'календарная'
				else 'целевая'	
			end as type,
			t2.target_prod, 
			t2.target_vas,
			'' as target_option, -- потом будет доработка 
			t2.target_fix,
			'' as comments -- служебное поле 
			from aggregate_campaign t 
		    left join aggregate_product t2 on t.campaign_name_plan = t2.industry
		
) with data distributed randomly;
analyze aggregate_target_prod;



-- Добавляем поле channel
drop table if exists  aggregate_update_channel;
create temporary table aggregate_update_channel
                               with (appendonly=true, compresstype=zstd, compresslevel=1)
                               on commit preserve rows
                               as 
      (
			select *, case 
				when campaign_name like '%_потенц_%'
					then substring(campaign_name, 8, Position('_потенц_' in campaign_name)-8)
				when campaign_name like '%_сущ_%'
					then substring(campaign_name, 8, Position('_сущ_' in campaign_name)-8)
				when lower(campaign_name) like '%_welcome_%' 
					then 
						case
						when campaign_name like '%МПП МСБ%'
					    	then 'МПП МСБ'
					    when campaign_name like '%МПР МСБ%'
					    	then 'МПР МСБ'
					    when campaign_name like '%ТМ_БР%'
					    	then 'ТМ_БР'  
					    end
			end as channel
			  from aggregate_target_prod          
) with data distributed randomly;
analyze aggregate_update_channel;


-- обновляем поле 
update aggregate_update_channel set channel = 'ТМ'
where channel = 'ТМ_БР'; 

-- формируем поле сегмент
drop table if exists aggregate_segment;
create temporary table aggregate_segment
                               with (appendonly=true, compresstype=zstd, compresslevel=1)
                               on commit preserve rows
                               as 
      (
			select a.*, ch.segment, 
				   0 as "Scoring_BigData" from  aggregate_update_channel a
			left join crm_b2b_sb.segment_channel ch on ch.channel = a.channel 
) with data distributed randomly;
analyze aggregate_segment;

-- формируем финальную таблицу, добавляя поля base_type
drop table if exists temp_desc;
create temporary table temp_desc
                               with (appendonly=true, compresstype=zstd, compresslevel=1)
                               on commit preserve rows
                               as 
      ( select *, case 
      				when campaign_name like '%_сущ_%'
      					then 'сущ'
      				when campaign_name like '%_потенц_%'
      					then 'потенц'
      				else 'сущ'
      end as base_type
	  from aggregate_segment
) with data distributed randomly;
analyze temp_desc;


truncate table crm_b2b_sb.desc;

insert into crm_b2b_sb.desc
select * from temp_desc;

