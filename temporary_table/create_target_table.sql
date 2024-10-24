

-- связь справочника аккаунта смс с целевой таблицей

drop table if exists  accounts_sms;
create temporary table accounts_sms
		select distinct 
      t1.regid
	, t1.app_n
		from 
		   (
			select 
				a.regid
				, a.app_n
				, b.product_pl_new
			from schema_name.detail_analit_mob_santalov as a -- заменить таблицу как будет переименнована основная таблица 
			left join schema_name.guide_accounts_sms as b
				on round(a.group_id, 0) = round(b.group_id, 0) 
				and left(a.sub_acc_trans, 8) = b.sub_acc_trans
			) t1
		where lower(t1.product_pl_new) = 'моб'
		) with data distributed randomly;
analyze accounts_sms;


-- присоединение по 


drop table if exists  software;
create temporary table software
		select distinct 
		t1.regid
		, t1.app_n
		, t1.con_typ1
		, date_format(to_date(t1.table_business_month), 'yyyyMM') as period
	from schema_name.detail_analit_mob_santalov t1
	where t1.regid||'-'||t1.app_n in
		   (
			select distinct
				a.regid||'-'||a.app_n
			from accounts_sms a 
			)
		) with data distributed randomly;
analyze software; 


-- дополняем таблицу с по 

drop table if exists  software_dlc;
create temporary table software_dlc
		select distinct 
      t1.regid
	, t1.app_n
    , t1.con_typ1
    , t2.segment as segment
	, t1.period
from software t1
left join schema_name.guide_segment t2 -- справочник сегментов 
	on lower(t1.con_typ1) = lower(t2.m_categ)
    )
		) with data distributed randomly;
analyze software_dlc; 


--- заполняются данные в таблицу schema_name.guide_vr_mob (у нас она уже создана и наполнена)


--- создается таблица с периодами 
drop table if exists  vr_mob_po_period ;
create temporary table vr_mob_po_period
		SELECT 
      a1.*
    , a2.period
FROM (select distinct b.regid, b.app_n from schema_name.guide_vr_mob as b) as a1
CROSS JOIN schema_name.table_period as a2   -- справочник периодов (необходимо подумать о том, как отказаться от этой таблицы)
		) with data distributed randomly;
analyze vr_mob_po_period; 


-- считаем деньги

drop table if exists  table_money ;
create temporary table table_money
				SELECT 
			t1.regid 
			, t1.app_n 
			, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
			, t2.product_pl_new
			, sum(t1.fclc_trans_part) as fclc_mob_total 
		FROM schema_name.detail_analit_mob_santalov as t1 -- поменять название позже
		left join schema_name.guide_accounts_sms as t2 -- справочник аккаунтов смс
				on round(t1.group_id, 0) = round(t2.group_id, 0) 
				and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
		WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
			and lower(t1.service_id1) not like '%штраф%'
			and lower(t1.service_id1) not like '%дебет%коррект%'
			and lower(t2.product_pl_new) = 'моб'
			and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01') -- почему?
		group by
			t1.regid
			, t1.app_n
			, period
			, t2.product_pl_new   
		) with data distributed randomly;
analyze table_money; 



-- считаем деньги без vas

drop table if exists  table_money_no_vas ;
create temporary table table_money_no_vas
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as sum_fclc_no_vas 
			FROM schema_name.detail_analit_mob_santalov as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) <> 'прочие vas'
				and lower(t1.service_id1) not like '%mobile%сервис%'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
				and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new  
		) with data distributed randomly;
analyze table_money_no_vas;


--- считаем prochm деньги 
drop table if exists  table_money_prochm ;
create temporary table table_money_prochm
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as prochie_mob 
			FROM schema_name.detail_analit_mob_santalov  as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'прочие моб'
				and lower(t1.service_id1) not like '%mobile%сервис%'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
				and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new 
		) with data distributed randomly;
analyze table_money_prochm;


--- считаем prochv деньги


drop table if exists  table_money_prochv ;
create temporary table table_money_prochv
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as prochie_vas 
			FROM schema_name.detail_analit_mob_santalov  as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and (lower(t2.product_pl_det2) = 'прочие vas'
				or lower(t1.service_id1) like '%mobile%сервис%'
				or lower(t1.service_id1) like 'b2b%оборудование%обслуживание%новая%цена%')
				and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new 
		) with data distributed randomly;
analyze table_money_prochv;


--- считаем gprs деньги

drop table if exists  table_money_gprs ;
create temporary table table_money_gprs
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as gprs 
			FROM schema_name.detail_analit_mob_santalov  as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'gprs'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
				and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new 
		) with data distributed randomly;
analyze table_money_gprs;


-- считаем golos деньги

drop table if exists  table_money_golos ;
create temporary table table_money_golos
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as golos 
			FROM  schema_name.detail_analit_mob_santalov  as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'голос'
				and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new 
		) with data distributed randomly;
analyze table_money_golos;


-- считаем sms деньги

drop table if exists  table_money_sms ;
create temporary table table_money_sms
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as sms 
			FROM schema_name.detail_analit_mob_santalov  as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'sms'
				and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze table_money_sms;


-- считаем vsr деньги

drop table if exists  table_money_vsr ;
create temporary table table_money_vsr
				SELECT 
				  t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as vsr 
			FROM  schema_name.detail_analit_mob_santalov  as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'вср'
				and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze table_money_vsr;



-- считаем vz деньги

drop table if exists  table_money_vz ;
create temporary table table_money_vz
				SELECT 
				t1.regid 
			  , t1.app_n 
			  , date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
	          , t2.product_pl_new
              , sum(t1.fclc_trans_part) as vz_mg_mn 
				FROM  schema_name.detail_analit_mob_santalov  as t1
				left join schema_name.guide_accounts_sms as t2
						on round(t1.group_id, 0) = round(t2.group_id, 0) 
						and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
				WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
					and lower(t1.service_id1) not like '%штраф%'
					and lower(t1.service_id1) not like '%дебет%коррект%'
					and lower(t2.product_pl_new) = 'моб'
					and lower(t2.product_pl_det2) = 'вз-мг-мн'
					and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
				group by
					t1.regid
					, t1.app_n
					, period
					, t2.product_pl_new
		) with data distributed randomly;
analyze table_money_vz;


-- считаем abon деньги

drop table if exists  table_money_abon ;
create temporary table table_money_abon
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as abon_plata 
			FROM  schema_name.detail_analit_mob_santalov  as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'абон.плата'
				and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze table_money_abon;


-- считаем total деньги

drop table if exists  table_money_total ;
create temporary table table_money_total
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, sum(t1.fclc_trans_part) as fclc_total 
			FROM  schema_name.detail_analit_mob_santalov  as t1
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a)
			and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
			group by
				t1.regid
				, t1.app_n
				, period
		) with data distributed randomly;
analyze table_money_total;

-- считаем aff_serv деньги

drop table if exists  table_money_aff_serv ;
create temporary table table_money_aff_serv
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, sum(t1.fclc_trans_part) as fclc_aff_serv 
			FROM schema_name.detail_analit_mob_santalov  as t1
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a)
				and (lower(t1.service_id1) like '%добровольная%блокировка%'
					or lower(t1.service_id1) like '%ежесуточная%плата%по%тарифу%180%дней%' 
					or lower(t1.service_id1) like '%ежесуточная%плата%по%тарифу%90%дней%')
				and t1.table_business_month not in ('2023-01-01', '2023-02-01', '2023-03-01')
			group by
				t1.regid
				, t1.app_n
				, period
		) with data distributed randomly;
analyze table_money_aff_serv;


-- считаем получаем данные за  202301

drop table if exists  table_correct_202301 ;
create temporary table table_correct_202301
				select
				t1.table_business_month
				, t1.regid
				, t1.app_n
				, t1.network_operator_id
				, t1.sub_acc_trans
				, t1.fclc_trans_part
				, t1.service_id1
				, t1.group_id
			from  schema_name.detail_analit_mob_santalov  t1
			where t1.table_business_month = '2023-01-01'
			 and left(t1.sub_acc_trans, 8) IN 
				   (
					select distinct b.sub_acc_trans from schema_name.guide_accounts_sms as b where b.product_pl_new = "Моб"
					)
		) with data distributed randomly;
analyze table_correct_202301;


--- считаем получаем данные за  202301_1

drop table if exists  table_correct_202301_1 ;
create temporary table table_correct_202301_1
				select
				t1.table_business_month
				, t1.regid
				, t1.app_n
				, t1.network_operator_id
				, t1.sub_acc_trans
				, t1.fclc_trans_part
				, t1.service_id1
				, t1.group_id
				, t2.place_of_call as place_of_call
			from table_correct_202301 t1
			left join schema_name.guide_202301_netw_pl_call t2
				on round(t1.network_operator_id, 3) = round(t2.network_operator_id, 3)
		) with data distributed randomly;
analyze table_correct_202301_1;


--- считаем получаем данные за  202301_2

drop table if exists  table_correct_202301_2 ;
create temporary table table_correct_202301_2
				select
				t1.table_business_month
				, t1.regid
				, t1.app_n
				, t1.network_operator_id
				, t1.sub_acc_trans
				, t1.fclc_trans_part
				, t1.service_id1
				, t1.group_id
				, t1.place_of_call
			from table_correct_202301_1 t1
			where t1.place_of_call <> 4 or t1.place_of_call is null
		) with data distributed randomly;
analyze table_correct_202301_2;


--- считаем vr_mob_money

drop table if exists  vr_mob_money ;
create temporary table vr_mob_money
				SELECT 
    t1.regid 
    , t1.app_n 
	, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
	, t2.product_pl_new
    , sum(t1.fclc_trans_part) as fclc_mob_total 
FROM  table_correct_202301_2 as t1
left join """ + sch + """ as t2
		on round(t1.group_id, 0) = round(t2.group_id, 0) 
		and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
    and lower(t1.service_id1) not like '%штраф%'
    and lower(t1.service_id1) not like '%дебет%коррект%'
	and lower(t2.product_pl_new) = 'моб'
group by
    t1.regid
    , t1.app_n
    , period
	, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money;


--- считаем vr_mob_money_no_vas

drop table if exists  vr_mob_money_no_vas ;
create temporary table vr_mob_money_no_vas
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as sum_fclc_no_vas 
			FROM table_correct_202301_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) <> 'прочие vas'
				and lower(t1.service_id1) not like '%mobile%сервис%'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_no_vas;

--- считаем vr_mob_money_prochm


drop table if exists  vr_mob_money_prochm ;
create temporary table vr_mob_money_prochm
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as prochie_mob 
			FROM table_correct_202301_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'прочие моб'
				and lower(t1.service_id1) not like '%mobile%сервис%'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_prochm;


--- считаем vr_mob_money_prochv


drop table if exists  vr_mob_money_prochv ;
create temporary table vr_mob_money_prochv
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as prochie_vas 
			FROM table_correct_202301_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and (lower(t2.product_pl_det2) = 'прочие vas'
				or lower(t1.service_id1) like '%mobile%сервис%'
				or lower(t1.service_id1) like 'b2b%оборудование%обслуживание%новая%цена%')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_prochv;


--- считаем vr_mob_money_gprs


drop table if exists  vr_mob_money_gprs ;
create temporary table vr_mob_money_gprs
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as gprs 
			FROM table_correct_202301_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'gprs'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_gprs;


--- считаем vr_mob_money_golos


drop table if exists  vr_mob_money_golos ;
create temporary table vr_mob_money_golos
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as golos 
			FROM table_correct_202301_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM  schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'голос'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_golos;


--- считаем vr_mob_money_sms


drop table if exists  vr_mob_money_sms ;
create temporary table vr_mob_money_sms
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as sms 
			FROM table_correct_202301_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'sms'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_sms;


--- считаем vr_mob_money_vsr

drop table if exists  vr_mob_money_vsr ;
create temporary table vr_mob_money_vsr
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as vsr 
			FROM table_correct_202301_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'вср'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_vsr;


--- считаем vr_mob_money_vz

drop table if exists  vr_mob_money_vz ;
create temporary table vr_mob_money_vz
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as vz_mg_mn 
			FROM table_correct_202301_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'вз-мг-мн'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_vz;



--- считаем vr_mob_money_abon

drop table if exists  vr_mob_money_abon ;
create temporary table vr_mob_money_abon
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as vz_mg_mn 
			FROM table_correct_202301_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'абон.плата'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_abon;


--- считаем vr_mob_money_total

drop table if exists  vr_mob_money_total ;
create temporary table vr_mob_money_total
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, sum(t1.fclc_trans_part) as fclc_total 
			FROM schema_name.detail_analit_mob_santalov as t1
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a)
				and t1.table_business_month = '2023-01-01'
			group by
				t1.regid
				, t1.app_n
				, period
		) with data distributed randomly;
analyze vr_mob_money_total;

--- считаем vr_mob_money_aff_serv

drop table if exists  vr_mob_money_aff_serv ;
				SELECT 
					  t1.regid 
					, t1.app_n 
					, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
					, sum(t1.fclc_trans_part) as fclc_aff_serv 
				FROM table_correct_202301_2 as t1
				WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a)
					and (lower(t1.service_id1) like '%добровольная%блокировка%'
						or lower(t1.service_id1) like '%ежесуточная%плата%по%тарифу%180%дней%' 
						or lower(t1.service_id1) like '%ежесуточная%плата%по%тарифу%90%дней%')
				group by
					t1.regid
					, t1.app_n
					, period
		) with data distributed randomly;
analyze vr_mob_money_aff_serv;

-- считаем table_correct_202302
drop table if exists  table_correct_202302 ;
create temporary table table_correct_202302
				select
				t1.table_business_month
				, t1.regid
				, t1.app_n
				, t1.network_operator_id
				, t1.sub_acc_trans
				, t1.fclc_trans_part
				, t1.service_id1
				, t1.group_id
			from  schema_name.detail_analit_mob_santalov  t1
			where t1.table_business_month = '2023-02-01'
			 and left(t1.sub_acc_trans, 8) IN 
				   (
					select distinct b.sub_acc_trans from schema_name.guide_accounts_sms as b where b.product_pl_new = "Моб"
					)
		) with data distributed randomly;
analyze table_correct_202302;


-- считаем table_correct_202302_1
drop table if exists  table_correct_202302_1 ;
create temporary table table_correct_202302_1 
				select
				t1.table_business_month
				, t1.regid
				, t1.app_n
				, t1.network_operator_id
				, t1.sub_acc_trans
				, t1.fclc_trans_part
				, t1.service_id1
				, t1.group_id
				, t2.place_of_call as place_of_call
			from table_correct_202302 t1
			left join schema_name.guide_202302_netw_pl_call t2
				on round(t1.network_operator_id, 3) = round(t2.network_operator_id, 3)
		) with data distributed randomly;
analyze table_correct_202302_1;

-- считаем table_correct_202302_2
drop table if exists  table_correct_202302_2 ;
create temporary table table_correct_202302_2 
				select
				t1.table_business_month
				, t1.regid
				, t1.app_n
				, t1.network_operator_id
				, t1.sub_acc_trans
				, t1.fclc_trans_part
				, t1.service_id1
				, t1.group_id
				, t1.place_of_call
			from table_correct_202302_1 t1
			where t1.place_of_call <> 4 or t1.place_of_call is null
		) with data distributed randomly;
analyze table_correct_202302_2;



--- считаем vr_mob_money_02

drop table if exists  vr_mob_money_02 ;
create temporary table vr_mob_money_02
				SELECT 
    t1.regid 
    , t1.app_n 
	, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
	, t2.product_pl_new
    , sum(t1.fclc_trans_part) as fclc_mob_total 
FROM  table_correct_202302_2 as t1
left join """ + sch + """ as t2
		on round(t1.group_id, 0) = round(t2.group_id, 0) 
		and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
    and lower(t1.service_id1) not like '%штраф%'
    and lower(t1.service_id1) not like '%дебет%коррект%'
	and lower(t2.product_pl_new) = 'моб'
group by
    t1.regid
    , t1.app_n
    , period
	, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_02;


--- считаем vr_mob_money_no_vas_02

drop table if exists  vr_mob_money_no_vas_02 ;
create temporary table vr_mob_money_no_vas_02
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as sum_fclc_no_vas 
			FROM table_correct_202302_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) <> 'прочие vas'
				and lower(t1.service_id1) not like '%mobile%сервис%'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_no_vas_02;

--- считаем vr_mob_money_prochm_02


drop table if exists  vr_mob_money_prochm_02 ;
create temporary table vr_mob_money_prochm_02
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as prochie_mob 
			FROM table_correct_202302_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'прочие моб'
				and lower(t1.service_id1) not like '%mobile%сервис%'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_prochm_02;


--- считаем vr_mob_money_prochv_02


drop table if exists  vr_mob_money_prochv_02 ;
create temporary table vr_mob_money_prochv_02
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as prochie_vas 
			FROM table_correct_202302_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and (lower(t2.product_pl_det2) = 'прочие vas'
				or lower(t1.service_id1) like '%mobile%сервис%'
				or lower(t1.service_id1) like 'b2b%оборудование%обслуживание%новая%цена%')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_prochv_02;


--- считаем vr_mob_money_gprs_02


drop table if exists  vr_mob_money_gprs_02 ;
create temporary table vr_mob_money_gprs_02
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as gprs 
			FROM table_correct_202302_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'gprs'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_gprs_02;


--- считаем vr_mob_money_golos_02


drop table if exists  vr_mob_money_golos_02 ;
create temporary table vr_mob_money_golos_02
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as golos 
			FROM table_correct_202302_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM  schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'голос'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_golos_02;


--- считаем vr_mob_money_sms_02


drop table if exists  vr_mob_money_sms_02 ;
create temporary table vr_mob_money_sms_02
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as sms 
			FROM table_correct_202302_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'sms'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_sms_02;


--- считаем vr_mob_money_vsr_02

drop table if exists  vr_mob_money_vsr_02 ;
create temporary table vr_mob_money_vsr_02
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as vsr 
			FROM table_correct_202302_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'вср'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_vsr_02;


--- считаем vr_mob_money_vz_02

drop table if exists  vr_mob_money_vz_02 ;
create temporary table vr_mob_money_vz_02
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as vz_mg_mn 
			FROM table_correct_202302_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'вз-мг-мн'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_vz_02;



--- считаем vr_mob_money_abon_02

drop table if exists  vr_mob_money_abon_02 ;
create temporary table vr_mob_money_abon_02
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as vz_mg_mn 
			FROM table_correct_202302_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'абон.плата'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_abon_02;


--- считаем vr_mob_money_total_02

drop table if exists  vr_mob_money_total_02 ;
create temporary table vr_mob_money_total_02
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, sum(t1.fclc_trans_part) as fclc_total 
			FROM schema_name.detail_analit_mob_santalov as t1
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a)
				and t1.table_business_month = '2023-02-01'
			group by
				t1.regid
				, t1.app_n
				, period
		) with data distributed randomly;
analyze vr_mob_money_total_02;

--- считаем vr_mob_money_aff_serv_02

drop table if exists  vr_mob_money_aff_serv_02 ;
				SELECT 
					  t1.regid 
					, t1.app_n 
					, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
					, sum(t1.fclc_trans_part) as fclc_aff_serv 
				FROM table_correct_202302_2 as t1
				WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a)
					and (lower(t1.service_id1) like '%добровольная%блокировка%'
						or lower(t1.service_id1) like '%ежесуточная%плата%по%тарифу%180%дней%' 
						or lower(t1.service_id1) like '%ежесуточная%плата%по%тарифу%90%дней%')
				group by
					t1.regid
					, t1.app_n
					, period
		) with data distributed randomly;
analyze vr_mob_money_aff_serv_02;


-- 

drop table if exists  vr_mob_money_aff_serv_02 ;
				SELECT 
					  t1.regid 
					, t1.app_n 
					, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
					, sum(t1.fclc_trans_part) as fclc_aff_serv 
				FROM table_correct_202302_2 as t1
				WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a)
					and (lower(t1.service_id1) like '%добровольная%блокировка%'
						or lower(t1.service_id1) like '%ежесуточная%плата%по%тарифу%180%дней%' 
						or lower(t1.service_id1) like '%ежесуточная%плата%по%тарифу%90%дней%')
				group by
					t1.regid
					, t1.app_n
					, period
		) with data distributed randomly;
analyze vr_mob_money_aff_serv_02;


-- считаем table_correct_202303
drop table if exists  table_correct_202303 ;
create temporary table table_correct_202303
				select
				t1.table_business_month
				, t1.regid
				, t1.app_n
				, t1.network_operator_id
				, t1.sub_acc_trans
				, t1.fclc_trans_part
				, t1.service_id1
				, t1.group_id
			from  schema_name.detail_analit_mob_santalov  t1
			where t1.table_business_month = '2023-03-01'
			 and left(t1.sub_acc_trans, 8) IN 
				   (
					select distinct b.sub_acc_trans from schema_name.guide_accounts_sms as b where b.product_pl_new = "Моб"
					)
		) with data distributed randomly;
analyze table_correct_202302;


-- считаем table_correct_202303_1
drop table if exists  table_correct_202303_1 ;
create temporary table table_correct_202303_1 
				select
				t1.table_business_month
				, t1.regid
				, t1.app_n
				, t1.network_operator_id
				, t1.sub_acc_trans
				, t1.fclc_trans_part
				, t1.service_id1
				, t1.group_id
				, t2.place_of_call as place_of_call
			from table_correct_202303 t1
			left join schema_name.guide_202303_netw_pl_call t2
				on round(t1.network_operator_id, 3) = round(t2.network_operator_id, 3)
		) with data distributed randomly;
analyze table_correct_202303_1;

-- считаем table_correct_202303_2
drop table if exists  table_correct_202303_2 ;
create temporary table table_correct_202303_2 
				select
				t1.table_business_month
				, t1.regid
				, t1.app_n
				, t1.network_operator_id
				, t1.sub_acc_trans
				, t1.fclc_trans_part
				, t1.service_id1
				, t1.group_id
				, t1.place_of_call
			from table_correct_202303_1 t1
			where t1.place_of_call <> 4 or t1.place_of_call is null
		) with data distributed randomly;
analyze table_correct_202303_2;


--- считаем vr_mob_money_03

drop table if exists  vr_mob_money_03 ;
create temporary table vr_mob_money_03
				SELECT 
    t1.regid 
    , t1.app_n 
	, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
	, t2.product_pl_new
    , sum(t1.fclc_trans_part) as fclc_mob_total 
FROM  table_correct_202303_2 as t1
left join """ + sch + """ as t2
		on round(t1.group_id, 0) = round(t2.group_id, 0) 
		and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
    and lower(t1.service_id1) not like '%штраф%'
    and lower(t1.service_id1) not like '%дебет%коррект%'
	and lower(t2.product_pl_new) = 'моб'
group by
    t1.regid
    , t1.app_n
    , period
	, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_03;


--- считаем vr_mob_money_no_vas_03

drop table if exists  vr_mob_money_no_vas_03 ;
create temporary table vr_mob_money_no_vas_03
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as sum_fclc_no_vas 
			FROM table_correct_202303_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) <> 'прочие vas'
				and lower(t1.service_id1) not like '%mobile%сервис%'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_no_vas_03;

--- считаем vr_mob_money_prochm_03


drop table if exists  vr_mob_money_prochm_03 ;
create temporary table vr_mob_money_prochm_03
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as prochie_mob 
			FROM table_correct_202303_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'прочие моб'
				and lower(t1.service_id1) not like '%mobile%сервис%'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_prochm_03;


--- считаем vr_mob_money_prochv_03


drop table if exists  vr_mob_money_prochv_03 ;
create temporary table vr_mob_money_prochv_03
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as prochie_vas 
			FROM table_correct_202303_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and (lower(t2.product_pl_det2) = 'прочие vas'
				or lower(t1.service_id1) like '%mobile%сервис%'
				or lower(t1.service_id1) like 'b2b%оборудование%обслуживание%новая%цена%')
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_prochv_03;


--- считаем vr_mob_money_gprs_03


drop table if exists  vr_mob_money_gprs_03 ;
create temporary table vr_mob_money_gprs_03
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as gprs 
			FROM table_correct_202303_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'gprs'
				and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_gprs_03;


--- считаем vr_mob_money_golos_03


drop table if exists  vr_mob_money_golos_03 ;
create temporary table vr_mob_money_golos_03
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as golos 
			FROM table_correct_202303_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM  schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'голос'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_golos_03;


--- считаем vr_mob_money_sms_03


drop table if exists  vr_mob_money_sms_03 ;
create temporary table vr_mob_money_sms_03
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as sms 
			FROM table_correct_202303_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'sms'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_sms_03;


--- считаем vr_mob_money_vsr_03

drop table if exists  vr_mob_money_vsr_03 ;
create temporary table vr_mob_money_vsr_03
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as vsr 
			FROM table_correct_202303_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'вср'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_vsr_03;


--- считаем vr_mob_money_vz_03

drop table if exists  vr_mob_money_vz_03 ;
create temporary table vr_mob_money_vz_03
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as vz_mg_mn 
			FROM table_correct_202303_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'вз-мг-мн'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_vz_03;



--- считаем vr_mob_money_abon_03

drop table if exists  vr_mob_money_abon_03 ;
create temporary table vr_mob_money_abon_03
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, t2.product_pl_new
				, sum(t1.fclc_trans_part) as vz_mg_mn 
			FROM table_correct_202303_2 as t1
			left join schema_name.guide_accounts_sms as t2
					on round(t1.group_id, 0) = round(t2.group_id, 0) 
					and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a) 
				and lower(t1.service_id1) not like '%штраф%'
				and lower(t1.service_id1) not like '%дебет%коррект%'
				and lower(t2.product_pl_new) = 'моб'
				and lower(t2.product_pl_det2) = 'абон.плата'
			group by
				t1.regid
				, t1.app_n
				, period
				, t2.product_pl_new
		) with data distributed randomly;
analyze vr_mob_money_abon_03;


--- считаем vr_mob_money_total_03

drop table if exists  vr_mob_money_total_03 ;
create temporary table vr_mob_money_total_03
				SELECT 
				t1.regid 
				, t1.app_n 
				, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
				, sum(t1.fclc_trans_part) as fclc_total 
			FROM schema_name.detail_analit_mob_santalov as t1
			WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a)
				and t1.table_business_month = '2023-03-01'
			group by
				t1.regid
				, t1.app_n
				, period
		) with data distributed randomly;
analyze vr_mob_money_total_03;

--- считаем vr_mob_money_aff_serv_03

drop table if exists  vr_mob_money_aff_serv_03 ;
create temporary table vr_mob_money_aff_serv_03
				SELECT 
					  t1.regid 
					, t1.app_n 
					, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
					, sum(t1.fclc_trans_part) as fclc_aff_serv 
				FROM table_correct_202303_2 as t1
				WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM schema_name.guide_vr_mob a)
					and (lower(t1.service_id1) like '%добровольная%блокировка%'
						or lower(t1.service_id1) like '%ежесуточная%плата%по%тарифу%180%дней%' 
						or lower(t1.service_id1) like '%ежесуточная%плата%по%тарифу%90%дней%')
				group by
					t1.regid
					, t1.app_n
					, period
		) with data distributed randomly;
analyze vr_mob_money_aff_serv_03;

-- считаем vr_mob_money_end

drop table if exists  vr_mob_money_end ;
create temporary table vr_mob_money_end
				SELECT 
					t1.*
				FROM table_money as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_end;


-- считаем vr_mob_money_no_vas_end

drop table if exists  vr_mob_money_no_vas_end ;
create temporary table vr_mob_money_no_vas_end
				SELECT 
					t1.*
				FROM table_money_no_vas as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_no_vas  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_no_vas_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_no_vas_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_no_vas_end;


-- считаем vr_mob_money_prochm_end

drop table if exists  vr_mob_money_prochm_end ;
create temporary table vr_mob_money_prochm_end
				SELECT 
					t1.*
				FROM table_money_prochm as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_prochm  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_prochm_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_prochm_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_prochm_end;


-- считаем vr_mob_money_prochv_end


drop table if exists  vr_mob_money_prochv_end ;
create temporary table vr_mob_money_prochv_end
				SELECT 
					t1.*
				FROM table_money_prochv as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_prochv  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_prochv_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_prochv_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_prochv_end;


-- считаем vr_mob_money_gprs_end


drop table if exists  vr_mob_money_gprs_end ;
create temporary table vr_mob_money_gprs_end
				SELECT 
					t1.*
				FROM table_money_gprs as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_gprs  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_gprs_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_gprs_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_gprs_end;


-- считаем vr_mob_money_golos_end


drop table if exists  vr_mob_money_golos_end ;
create temporary table vr_mob_money_golos_end
				SELECT 
					t1.*
				FROM table_money_golos as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_golos  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_golos_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_golos_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_golos_end;


-- считаем vr_mob_money_sms_end


drop table if exists  vr_mob_money_sms_end ;
create temporary table vr_mob_money_sms_end
				SELECT 
					t1.*
				FROM table_money_sms as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_sms  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_sms_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_sms_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_sms_end;

-- считаем vr_mob_money_vsr_end


drop table if exists  vr_mob_money_vsr_end ;
create temporary table vr_mob_money_vsr_end
				SELECT 
					t1.*
				FROM table_money_vsr as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_vsr  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_vsr_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_vsr_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_vsr_end;


-- считаем vr_mob_money_vz_end


drop table if exists  vr_mob_money_vz_end ;
create temporary table vr_mob_money_vz_end
				SELECT 
					t1.*
				FROM table_money_vz as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_vz  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_vz_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_vz_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_vz_end;


-- считаем vr_mob_money_abon_end


drop table if exists  vr_mob_money_abon_end ;
create temporary table vr_mob_money_abon_end
				SELECT 
					t1.*
				FROM table_money_abon as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_abon  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_abon_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_abon_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_abon_end;


-- считаем vr_mob_money_aff_serv_end


drop table if exists  vr_mob_money_aff_serv_end ;
create temporary table vr_mob_money_aff_serv_end
				SELECT 
					t1.*
				FROM table_money_aff_serv as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_aff_serv  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_aff_serv_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_aff_serv_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_aff_serv_end;


-- считаем vr_mob_money_aff_serv_end


drop table if exists  vr_mob_money_aff_serv_end ;
create temporary table vr_mob_money_aff_serv_end
				SELECT 
					t1.*
				FROM table_money_aff_serv as t1
				union all
				SELECT 
					t2.*
				FROM vr_mob_money_aff_serv  as t2
				union all
				SELECT 
					t3.*
				FROM vr_mob_money_aff_serv_02 as t3
				union all
				SELECT 
					t4.*
				FROM vr_mob_money_aff_serv_03 as t4
		) with data distributed randomly;
analyze vr_mob_money_aff_serv_end;


-- создаем 1 врем

drop table if exists vrem_1 ;
create temporary table vrem_1 
	SELECT 
    t1.period
    , t1.regid
    , t1.app_n
    , t2.prochie_mob
    , t3.prochie_vas
    , t4.gprs
    , t5.golos
    , t6.sms
    , t7.vsr
    , t8.vz_mg_mn
    , t9.abon_plata
    , t10.fclc_mob_total as sum_fclc
    , t11.sum_fclc_no_vas
    , t12.fclc_total
    , t13.fclc_aff_serv
FROM vr_mob_po_period as t1
LEFT JOIN vr_mob_money_prochm_end as t2
    ON t1.app_n = t2.app_n 
    and t1.regid = t2.regid
    and t1.period = t2.period
LEFT JOIN vr_mob_money_prochv_end as t3
    ON t1.app_n = t3.app_n 
    and t1.regid = t3.regid
    and t1.period = t3.period
LEFT JOIN vr_mob_money_gprs_end as t4
    ON t1.app_n = t4.app_n 
    and t1.regid = t4.regid
    and t1.period = t4.period
LEFT JOIN vr_mob_money_golos_end as t5
    ON t1.app_n = t5.app_n 
    and t1.regid = t5.regid
    and t1.period = t5.period
LEFT JOIN vr_mob_money_sms_end as t6
    ON t1.app_n = t6.app_n 
    and t1.regid = t6.regid
    and t1.period = t6.period
LEFT JOIN vr_mob_money_vsr_end as t7
    ON t1.app_n = t7.app_n 
    and t1.regid = t7.regid
    and t1.period = t7.period
LEFT JOIN vr_mob_money_vz_end as t8
    ON t1.app_n = t8.app_n 
    and t1.regid = t8.regid
    and t1.period = t8.period
LEFT JOIN vr_mob_money_abon_end as t9
    ON t1.app_n = t9.app_n 
    and t1.regid = t9.regid
    and t1.period = t9.period
LEFT JOIN vr_mob_money_end as t10
    ON t1.app_n = t10.app_n 
    and t1.regid = t10.regid
    and t1.period = t10.period
LEFT JOIN vr_mob_money_no_vas_end as t11
    ON t1.app_n = t11.app_n 
    and t1.regid = t11.regid
    and t1.period = t11.period
left join vr_mob_money_total_end as t12
    ON t1.app_n = t12.app_n 
    and t1.regid = t12.regid
    and t1.period = t12.period
LEFT JOIN vr_mob_money_aff_serv_end as t13 
    ON t1.app_n = t13.app_n 
    and t1.regid = t13.regid
    and t1.period = t13.period
)with data distributed randomly;
analyze vrem_1;

-- создаем 2 врем

drop table if exists vrem_2 ;
create temporary table vrem_2 
	SELECT distinct
    date_format(to_date(t1.table_business_month), 'yyyyMM') as period
    , t1.regid 
    , t1.app_n 
    , t1.phone_num
    , t1.con_n 
	, t1.kontr_name 
	, t1.activation_date 
	, t1.region
    , t1.con_typ
	, t1.con_typ1 
    , t1.tariff_plan
	, t1.tariff_plan1 
    , t1.tp_group
	, t1.tp_group1
FROM schema_name.detail_analit_mob_santalov  
WHERE t1.regid||'-'||t1.app_n IN (select distinct d1.regid||'-'||d1.app_n from vrem_1 d1)
)with data distributed randomly;
analyze vrem_2;


-- создаем 3 врем

drop table if exists vrem_3 ;
create temporary table vrem_3 
	SELECT
    t1.period
    ,t1.regid
    ,t1.app_n
    ,t1.prochie_mob as fclc_prochie_mob1
    ,t1.prochie_vas as fclc_prochie_vas1
    ,t1.golos as fclc_golos1
    ,t1.gprs as fclc_gprs1
    ,t1.sms as fclc_sms1
    ,t1.vsr as fclc_vsr1
    ,t1.vz_mg_mn as fclc_vz_mg_mn1
    ,t1.abon_plata as fclc_abon_plata1
    ,t1.sum_fclc as fclc_mob_total1
    ,t1.sum_fclc_no_vas as fclc_mob_no_vas1
    ,t1.fclc_total as fclc_total1
    ,t1.fclc_aff_serv as fclc_aff_serv1
	,t2.phone_num
    ,t2.con_n 
	,t2.kontr_name 
	,t2.activation_date 
	,t2.region
    ,t2.con_typ
	,t2.con_typ1 
    ,t2.tariff_plan
	,t2.tariff_plan1 
    ,t2.tp_group
	,t2.tp_group1
from vrem_1 t1
left join vrem_2 t2
	on t1.period = t2.period
	and t1.regid = t2.regid
	and t1.app_n = t2.app_n 
)with data distributed randomly;
analyze vrem_3;


-- создаем 4 врем

drop table if exists vrem_4 ;
create temporary table vrem_4 
	SELECT
    t1.* 
	, CASE
        when t1.con_n is NULL 
            then first_value(t1.con_n) over (partition by t1.regid, t1.app_n, t1.part_con_n order by t1.period desc)
        else t1.con_n
        end as con_n_x
	, CASE
        when t1.kontr_name is NULL 
            then first_value(t1.kontr_name) over (partition by t1.regid, t1.app_n, t1.part_kontr_name order by t1.period desc)
        else t1.kontr_name
        end as kontr_name_x
	, CASE
        when t1.region is NULL 
            then first_value(t1.region) over (partition by t1.regid, t1.app_n, t1.part_region order by t1.period desc)
        else t1.region
        end as region_x
	, CASE
        when t1.activation_date is NULL 
            then first_value(t1.activation_date) over (partition by t1.regid, t1.app_n, t1.part_activation_date order by t1.period desc)
        else t1.activation_date
        end as activation_date_x
	, CASE
        when t1.tariff_plan is NULL 
            then first_value(t1.tariff_plan) over (partition by t1.regid, t1.app_n, t1.part_tariff_plan order by t1.period desc)
        else t1.tariff_plan
        end as tariff_plan_x
	, CASE
        when t1.tp_group is NULL 
            then first_value(t1.tp_group) over (partition by t1.regid, t1.app_n, t1.part_tp_group order by t1.period desc)
        else t1.tp_group
        end as tp_group_x
	, CASE
        when t1.con_typ is NULL 
            then first_value(t1.con_typ) over (partition by t1.regid, t1.app_n, t1.part_con_typ order by t1.period desc)
        else t1.con_typ
        end as con_typ_x
    

	, CASE
        when t1.tariff_plan1 is NULL 
            then first_value(t1.tariff_plan1) over (partition by t1.regid, t1.app_n, t1.part_tariff_plan1 order by t1.period desc)
        else t1.tariff_plan1
        end as tariff_plan1_x
	, CASE
        when t1.tp_group1 is NULL 
            then first_value(t1.tp_group1) over (partition by t1.regid, t1.app_n, t1.part_tp_group1 order by t1.period desc)
        else t1.tp_group1
        end as tp_group1_x
	, CASE
        when t1.con_typ1 is NULL 
            then first_value(t1.con_typ1) over (partition by t1.regid, t1.app_n, t1.part_con_typ1 order by t1.period desc)
        else t1.con_typ1
        end as con_typ1_x
	, CASE
        when t1.phone_num is NULL 
            then first_value(t1.phone_num) over (partition by t1.regid, t1.app_n, t1.part_phone_num order by t1.period desc)
        else t1.phone_num
        end as phone_num_x    
FROM 
   (
    select 
        a.*
        , sum(case when a.con_n is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_con_n
		, sum(case when a.kontr_name is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_kontr_name
		, sum(case when a.region is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_region
		, sum(case when a.activation_date is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_activation_date
		, sum(case when a.tariff_plan is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_tariff_plan
		, sum(case when a.tp_group is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_tp_group
		, sum(case when a.con_typ is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_con_typ
        
		
        , sum(case when a.tariff_plan1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_tariff_plan1
		, sum(case when a.tp_group1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_tp_group1
		, sum(case when a.con_typ1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_con_typ1
		, sum(case when a.phone_num is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period desc) part_phone_num
    from vrem_3 as a
    ) as t1
)with data distributed randomly;
analyze vrem_4;


-- создаем 5 врем

drop table if exists vrem_5;
create temporary table vrem_5 
			SELECT
				t1.*
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.con_n_x
					ELSE t1.con_n
					end as con_n_x1
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.kontr_name_x
					ELSE t1.kontr_name
					end as kontr_name_x1
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.region_x
					ELSE t1.region
					end as region_x1
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.activation_date_x
					ELSE t1.activation_date
					end as activation_date_x1
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.tariff_plan_x
					ELSE t1.tariff_plan
					end as tariff_plan_x1 
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.tp_group_x
					ELSE t1.tp_group
					end as tp_group_x1 
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.con_typ_x
					ELSE t1.con_typ
					end as con_typ_x1
					
				
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.tariff_plan1_x
					ELSE t1.tariff_plan1
					end as tariff_plan1_x1 
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.tp_group1_x
					ELSE t1.tp_group1
					end as tp_group1_x1 
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.con_typ1_x
					ELSE t1.con_typ1
					end as con_typ1_x1
				, CASE
					when 
					   (
						row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)
						) = 1
						THEN t1.phone_num_x
					ELSE t1.phone_num
					end as phone_num_x1
			FROM vrem_4 as t1
)with data distributed randomly;
analyze vrem_5;


-- создаем 6 врем

drop table if exists vrem_6;
create temporary table vrem_6 
SELECT
    t1.* 
    , CASE
        when t1.con_n_x1 is NULL 
            then first_value(t1.con_n_x1) over (partition by t1.regid, t1.app_n, t1.part_con_n_x1 order by t1.period)
        else t1.con_n_x1
        end as con_n_x2 
	, CASE
        when t1.kontr_name_x1 is NULL 
            then first_value(t1.kontr_name_x1) over (partition by t1.regid, t1.app_n, t1.part_kontr_name_x1 order by t1.period)
        else t1.kontr_name_x1
        end as kontr_name_x2 
	, CASE
        when t1.region_x1 is NULL 
            then first_value(t1.region_x1) over (partition by t1.regid, t1.app_n, t1.part_region_x1 order by t1.period)
        else t1.region_x1
        end as region_x2 
	, CASE
        when t1.activation_date_x1 is NULL 
            then first_value(t1.activation_date_x1) over (partition by t1.regid, t1.app_n, t1.part_activation_date_x1 order by t1.period)
        else t1.activation_date_x1
        end as activation_date_x2 
	, CASE
        when t1.tariff_plan_x1 is NULL 
            then first_value(t1.tariff_plan_x1) over (partition by t1.regid, t1.app_n, t1.part_tariff_plan_x1 order by t1.period)
        else t1.tariff_plan_x1
        end as tariff_plan_x2 
	, CASE
        when t1.tp_group_x1 is NULL 
            then first_value(t1.tp_group_x1) over (partition by t1.regid, t1.app_n, t1.part_tp_group_x1 order by t1.period)
        else t1.tp_group_x1
        end as tp_group_x2 
	, CASE
        when t1.con_typ_x1 is NULL 
            then first_value(t1.con_typ_x1) over (partition by t1.regid, t1.app_n, t1.part_con_typ_x1 order by t1.period)
        else t1.con_typ_x1
        end as con_typ_x2
	
	
	, CASE
        when t1.tariff_plan1_x1 is NULL 
            then first_value(t1.tariff_plan1_x1) over (partition by t1.regid, t1.app_n, t1.part_tariff_plan1_x1 order by t1.period)
        else t1.tariff_plan1_x1
        end as tariff_plan1_x2 
	, CASE
        when t1.tp_group1_x1 is NULL 
            then first_value(t1.tp_group1_x1) over (partition by t1.regid, t1.app_n, t1.part_tp_group1_x1 order by t1.period)
        else t1.tp_group1_x1
        end as tp_group1_x2 
	, CASE
        when t1.con_typ1_x1 is NULL 
            then first_value(t1.con_typ1_x1) over (partition by t1.regid, t1.app_n, t1.part_con_typ1_x1 order by t1.period)
        else t1.con_typ1_x1
        end as con_typ1_x2
	, CASE
        when t1.phone_num_x1 is NULL 
            then first_value(t1.phone_num_x1) over (partition by t1.regid, t1.app_n, t1.part_con_typ1_x1 order by t1.period)
        else t1.phone_num_x1
        end as phone_num_x2
FROM 
   (
    select 
        a.*
        , sum(case when a.con_n_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_con_n_x1
		, sum(case when a.kontr_name_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_kontr_name_x1
		, sum(case when a.region_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_region_x1
		, sum(case when a.activation_date_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_activation_date_x1
		, sum(case when a.tariff_plan_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_tariff_plan_x1
		, sum(case when a.tp_group_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_tp_group_x1
		, sum(case when a.con_typ_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_con_typ_x1
			
		
		, sum(case when a.tariff_plan1_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_tariff_plan1_x1
		, sum(case when a.tp_group1_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_tp_group1_x1
		, sum(case when a.con_typ1_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_con_typ1_x1
		, sum(case when a.phone_num_x1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_phone_num_x1
    from vrem_5 as a
    ) as t1
)with data distributed randomly;
analyze vrem_6;



-- создаем 6_1 врем

drop table if exists vrem_6_1;
create temporary table vrem_6_1 
select
	t1.period
	,t1.regid
	,t1.app_n
	,t1.phone_num_x2
	,t1.fclc_prochie_mob1
	,t1.fclc_prochie_vas1
	,t1.fclc_golos1
	,t1.fclc_gprs1
	,t1.fclc_sms1
	,t1.fclc_vsr1
	,t1.fclc_vz_mg_mn1
	,t1.fclc_abon_plata1
	,t1.fclc_mob_total1
	,t1.fclc_mob_no_vas1
	,t1.fclc_total1
	,t1.fclc_aff_serv1
	,t1.con_n_x2 
	,t1.kontr_name_x2
	,t1.activation_date_x2
	,t1.region_x2
	,t1.con_typ_x2
	,t1.con_typ1_x2 
	,t1.tariff_plan_x2
	,t1.tariff_plan1_x2 
	,t1.tp_group_x2
	,case 
		when lower(t1.tariff_plan1_x2) in ('москва - комфорт xl - 250 мбит/с плюс тв 062022 (шпд) (eth) (впо) (фс) (scp)', 
		                                   'москва - комфорт xl - 500 мбит/с плюс тв (шпд) (eth) (впо) (фс) (scp)', 
										   'москва - комфорт xl - 500 мбит/с плюс тв 062022 (шпд) (eth) (впо) (фс) (scp)') 
			then 'Конвергент'
		when lower(t1.tariff_plan1_x2) = 'казань фс - базис интернет 500 мбит/с (шпд) (eth) (ск) (фс) (scp)' 
			then 'Интернет безлимитный масс'
		when lower(t1.tariff_plan1_x2) = 'казань фс - пакет услуг' 
			then 'Интернет+ТВ'
		else t1.tp_group1_x2 end as tp_group1_x2_1
from vrem_6 t1
    
)with data distributed randomly;
analyze vrem_6_1;


-- создаем 7 врем

drop table if exists vrem_7;
create temporary table vrem_7 
select 
    t2.period
	,t2.regid
	,t2.app_n
    ,t2.phone_num_x2
	,t2.fclc_prochie_mob1
	,t2.fclc_prochie_vas1
	,t2.fclc_golos1
	,t2.fclc_gprs1
	,t2.fclc_sms1
	,t2.fclc_vsr1
	,t2.fclc_vz_mg_mn1
	,t2.fclc_abon_plata1
	,t2.fclc_mob_total1
	,t2.fclc_mob_no_vas1
	,t2.fclc_total1
	,t2.fclc_aff_serv1
	,t2.con_n_x2 
	,t2.kontr_name_x2
	,t2.activation_date_x2
	,t2.region_x2
	,t3.region_txt as region_txt
	,t2.con_typ_x2
	,t2.con_typ1_x2 
	,t2.tariff_plan_x2
	,t2.tariff_plan1_x2 
	,t2.tp_group_x2
	,t2.tp_group1_x2
	,min(t2.activat_date_x1) OVER (PARTITION BY t2.regid, t2.app_n) as activat_date_x2
	,t4.segment as segment
	,t5.tp_group_big as tp_big
from 
   (
    select
        t1.period
		,t1.regid
		,t1.app_n
        ,t1.phone_num_x2
		,t1.fclc_prochie_mob1
		,t1.fclc_prochie_vas1
		,t1.fclc_golos1
		,t1.fclc_gprs1
		,t1.fclc_sms1
		,t1.fclc_vsr1
		,t1.fclc_vz_mg_mn1
		,t1.fclc_abon_plata1
		,t1.fclc_mob_total1
		,t1.fclc_mob_no_vas1
		,t1.fclc_total1
		,t1.fclc_aff_serv1
		,t1.con_n_x2 
		,t1.kontr_name_x2
		,t1.activation_date_x2
		,t1.region_x2
		,t1.con_typ_x2
		,t1.con_typ1_x2 
		,t1.tariff_plan_x2
		,t1.tariff_plan1_x2 
		,t1.tp_group_x2
		,t1.tp_group1_x2_1 as tp_group1_x2
		,date_format(to_date(t1.activation_date_x2), 'yyyyMM') as activat_date_x1
	from vrem_6_1 t1
    ) t2
left join schema_name.guide_regions_and_macroregions t3
	on t2.region_x2 = t3.region
left join schema_name.guide_segment t4
	on lower(t2.con_typ1_x2) = lower(t4.m_categ)
left join schema_name.guide_tp_group_big t5
	on lower(t2.tp_group1_x2) = lower(t5.tp_group)
    
)with data distributed randomly;
analyze vrem_7;



-- создаем 8 врем

drop table if exists vrem_8;
create temporary table vrem_8 
select
	t2.*
	, case 
        when (COALESCE(t2.fclc_mob_total2, 0)) > 0 
            then 1
            else 0
        end as activnost_mob_total
    , case 
        when (COALESCE(t2.fclc_mob_no_vas2, 0)) > 0 
            then 1
            else 0
        end as activnost_mob_no_vas
from 
   (
	select 
    t1.period
	,t1.regid
	,t1.app_n
	,t1.fclc_total1 as fclc_total2
	,t1.phone_num_x2
	,t1.con_n_x2 
	,t1.kontr_name_x2
	,t1.activation_date_x2
	,t1.region_x2
	,t1.region_txt
	,t1.con_typ_x2
	,t1.con_typ1_x2 
	,t1.tariff_plan_x2
	,t1.tariff_plan1_x2 
	,t1.tp_group_x2
	,t1.tp_group1_x2
	,t1.activat_date_x2
    ,case when t1.segment is null then 'B2C' else t1.segment end as segment1
	,t1.tp_big
    ,CASE 
        WHEN t1.activat_date_x2 = t1.period 
            then 1 
            else 0 
        end as activation_marker
    ,LAG(t1.tariff_plan_x2) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as tariff_plan_x2_prev
    ,LAG(t1.tariff_plan1_x2) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as tariff_plan1_x2_prev
	,CASE 
        WHEN t1.tariff_plan1_x2 <> LAG(t1.tariff_plan1_x2) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) 
            THEN 1 
            else 0 
        end as tariff_mgr
    ,LAG(t1.tp_group_x2) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as tp_group_x2_prev
	,LAG(t1.tp_group1_x2) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as tp_group1_x2_prev
    ,CASE 
        WHEN t1.tp_group1_x2 <> LAG(t1.tp_group1_x2) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) 
            THEN 1 
            else 0 
        end as tp_group_mgr
    ,LAG(t1.tp_big) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as tp_big_prev
    ,CASE 
        WHEN t1.tp_big <> LAG(t1.tp_big) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) 
            THEN 1 
            else 0 
        end as tp_big_mgr
    ,LAG(t1.con_typ_x2) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as con_typ_x2_prev
	,LAG(t1.con_typ1_x2) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as con_typ1_x2_prev
    ,CASE 
        WHEN t1.con_typ1_x2 <> LAG(t1.con_typ1_x2) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) 
            THEN 1 
            else 0 
        end as categ_mgr
    ,LAG(t1.segment) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as segment_prev
    ,CASE 
        WHEN t1.segment <> LAG(t1.segment) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) 
            THEN 1 
            else 0 
        end as segment_mgr
    ,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_prochie_mob1 end as fclc_prochie_mob2
    ,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_prochie_vas1 end as fclc_prochie_vas2
    ,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_golos1 end as fclc_golos2
    ,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_gprs1 end as fclc_gprs2
    ,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_sms1 end as fclc_sms2
    ,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_vsr1 end as fclc_vsr2
    ,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_vz_mg_mn1 end as fclc_vz_mg_mn2
    ,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_abon_plata1 end as fclc_abon_plata2
    ,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_mob_total1 end as fclc_mob_total2
    ,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_mob_no_vas1 end as fclc_mob_no_vas2
	,case 
        when t1.segment in ('B2C', 'Не в B2C') then null
        else t1.fclc_aff_serv1 end as fclc_aff_serv2
	from vrem_7 t1
	) t2
    
)with data distributed randomly;
analyze vrem_8;


-- создаем vrem_b2b

drop table if exists vrem_b2b;
create temporary table vrem_b2b 
		select distinct 
			t1.regid 
		, t1.app_n 
		, 'po_B2B' as is_B2B
		FROM vrem_8 as t1 
		where t1.segment1 not in ('B2C', 'Не в B2C')
    
)with data distributed randomly;
analyze vrem_b2b;

-- создаем vrem_9

drop table if exists vrem_9;
create temporary table vrem_9 
		select 
    t1.period
	,t1.regid
	,t1.app_n
	,t1.phone_num_x2
	,t1.con_n_x2
	,t3.inn as inn1	
	,t1.kontr_name_x2
	,t1.activation_date_x2
	,case 
        when t1.activat_date_x2 is NULL
            then '197001'
            else t1.activat_date_x2
		end as activat_date_x3
	,t1.activation_marker
	,t1.region_x2
	,t1.region_txt
	
	,t1.tariff_mgr
	,case 
		when (row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)) = 1 
			and t1.tariff_plan_x2_prev is NULL 
            then t1.tariff_plan_x2
		else t1.tariff_plan_x2_prev 
		end as tariff_plan_x3_prev
	,t1.tariff_plan_x2
	,case 
		when (row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)) = 1 
			and t1.tariff_plan1_x2_prev is NULL 
            then t1.tariff_plan1_x2
		else t1.tariff_plan1_x2_prev 
		end as tariff_plan1_x3_prev
	,t1.tariff_plan1_x2
	
	,t1.tp_group_mgr
	,case 
		when (row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)) = 1 
			and t1.tp_group_x2_prev is NULL 
            then t1.tp_group_x2
		else t1.tp_group_x2_prev 
		end as tp_group_x3_prev
	,t1.tp_group_x2
	,case 
		when (row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)) = 1 
			and t1.tp_group1_x2_prev is NULL 
            then t1.tp_group1_x2
		else t1.tp_group1_x2_prev 
		end as tp_group1_x3_prev
	,t1.tp_group1_x2
	
	,t1.tp_big_mgr
	,case 
		when (row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)) = 1 
			and t1.tp_big_prev is NULL 
            then t1.tp_big
		else t1.tp_big_prev 
		end as tp_big_prev1
	,t1.tp_big
		
	,t1.categ_mgr
	,case 
		when (row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)) = 1 
			and t1.con_typ_x2_prev is NULL 
            then t1.con_typ_x2
		else t1.con_typ_x2_prev 
		end as con_typ_x3_prev
	,t1.con_typ_x2
	,case 
		when (row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)) = 1 
			and t1.con_typ1_x2_prev is NULL 
            then t1.con_typ1_x2
		else t1.con_typ1_x2_prev 
		end as con_typ1_x3_prev
	,t1.con_typ1_x2
	
	,t1.segment_mgr
	,case 
		when (row_number() OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)) = 1 
			and t1.segment_prev is NULL 
            then t1.segment1
		else t1.segment_prev 
		end as segment_prev1
	,t1.segment1 as segment
	
	,t1.fclc_prochie_mob2
    ,t1.fclc_prochie_vas2
    ,t1.fclc_golos2
    ,t1.fclc_gprs2
    ,t1.fclc_sms2
    ,t1.fclc_vsr2
    ,t1.fclc_vz_mg_mn2
    ,t1.fclc_abon_plata2
    ,t1.fclc_mob_total2
    ,t1.fclc_mob_no_vas2
	,t1.fclc_total2
	,t1.fclc_aff_serv2
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	,t2.is_B2B as is_B2B
	,case 
        WHEN COALESCE(t1.fclc_mob_total2, 0) = 0 
			then '0'
        WHEN COALESCE(t1.fclc_mob_total2, 0) > 0
			and COALESCE(t1.fclc_mob_total2, 0) < 50 
			then '0_50'
        WHEN COALESCE(t1.fclc_mob_total2, 0) >= 50
			and COALESCE(t1.fclc_mob_total2, 0) < 100 
			then '50_100'
        WHEN COALESCE(t1.fclc_mob_total2, 0) >= 100
			and COALESCE(t1.fclc_mob_total2, 0) < 250 
			then '100_250'
        WHEN COALESCE(t1.fclc_mob_total2, 0) >= 250
			and COALESCE(t1.fclc_mob_total2, 0) < 500 
			then '250_500'
        WHEN COALESCE(t1.fclc_mob_total2, 0) >= 500 
			then '500_i_bolshe'
		END AS ARPU_categ_mob_total
    ,case 
        WHEN COALESCE(t1.fclc_mob_no_vas2, 0) = 0 
			then '0'
        WHEN COALESCE(t1.fclc_mob_no_vas2, 0) > 0
			and COALESCE(t1.fclc_mob_no_vas2, 0) < 50 
			then '0_50'
        WHEN COALESCE(t1.fclc_mob_no_vas2, 0) >= 50
			and COALESCE(t1.fclc_mob_no_vas2, 0) < 100 
			then '50_100'
        WHEN COALESCE(t1.fclc_mob_no_vas2, 0) >= 100
			and COALESCE(t1.fclc_mob_no_vas2, 0) < 250 
			then '100_250'
        WHEN COALESCE(t1.fclc_mob_no_vas2, 0) >= 250
			and COALESCE(t1.fclc_mob_no_vas2, 0) < 500 
			then '250_500'
        WHEN COALESCE(t1.fclc_mob_no_vas2, 0) >= 500 
			then '500_i_bolshe'
		END AS ARPU_categ_mob_no_vas
from vrem_8 t1
left join vrem_b2b t2
    on t1.regid = t2.regid 
    and t1.app_n = t2.app_n
LEFT JOIN 
       (
        SELECT DISTINCT 
            a3.regid
            , a3.con_n
            , a3.inn 
        FROM raw.mtsru_tdwh_smr_tdprd__uat_v_basefull_drkk_con_inn as a3 -- надо найти таблицу
        WHERE a3.is_closed = 0
            and a3.con_status = 1
        ) t3
    ON t1.regid = t3.regid
    and t1.con_n_x2 = t3.con_n
)with data distributed randomly;
analyze vrem_9;


-- создаем vrem_10

drop table if exists vrem_10;
create temporary table vrem_10 
		select 
    t1.period
	,t1.regid
	,t1.app_n
	,t1.phone_num_x2
	,t1.con_n_x2
	,case 
        when t1.inn1 is null 
			or t1.segment in ('B2C', 'Не в B2C') then t1.con_n_x2
            else t1.inn1
        end as inn2
	,t1.kontr_name_x2
	,t1.activation_date_x2
	,t1.activat_date_x3
	,t1.activation_marker
	,t1.region_x2
	,t1.region_txt
	
	,t1.tariff_mgr
	,case 
        when t1.activation_marker = 1 
            then t1.tariff_plan_x2
            else t1.tariff_plan_x3_prev
        end tariff_plan_x4_prev
	,t1.tariff_plan_x2
	,case 
        when t1.activation_marker = 1 
            then t1.tariff_plan1_x2
            else t1.tariff_plan1_x3_prev
        end tariff_plan1_x4_prev
	,t1.tariff_plan1_x2
	
	,t1.tp_group_mgr
	,case 
        when t1.activation_marker = 1 
            then t1.tp_group_x2
            else t1.tp_group_x3_prev
        end tp_group_x4_prev
	,t1.tp_group_x2
	,case 
        when t1.activation_marker = 1 
            then t1.tp_group1_x2
            else t1.tp_group1_x3_prev
        end tp_group1_x4_prev
	,t1.tp_group1_x2
	
	,t1.tp_big_mgr
	,case 
        when t1.activation_marker = 1 
            then t1.tp_big
            else t1.tp_big_prev1
        end tp_big_prev2
	,t1.tp_big
		
	,t1.categ_mgr
	,case 
        when t1.activation_marker = 1 
            then t1.con_typ_x2
            else t1.con_typ_x3_prev
        end con_typ_x4_prev
	,t1.con_typ_x2
	,case 
        when t1.activation_marker = 1 
            then t1.con_typ1_x2
            else t1.con_typ1_x3_prev
        end con_typ1_x4_prev
	,t1.con_typ1_x2
	
	,t1.segment_mgr
	,case 
        when t1.activation_marker = 1 
            then t1.segment
            else t1.segment_prev1
        end segment_prev2
	,t1.segment
	
	,t1.fclc_prochie_mob2
    ,t1.fclc_prochie_vas2
    ,t1.fclc_golos2
    ,t1.fclc_gprs2
    ,t1.fclc_sms2
    ,t1.fclc_vsr2
    ,t1.fclc_vz_mg_mn2
    ,t1.fclc_abon_plata2
    ,t1.fclc_mob_total2
    ,t1.fclc_mob_no_vas2
	,t1.fclc_total2
	,t1.fclc_aff_serv2
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	,t1.is_B2B
	,t1.ARPU_categ_mob_total
    ,t1.ARPU_categ_mob_no_vas
from vrem_9 t1
    
)with data distributed randomly;
analyze vrem_10;


-- создаем vrem_block

drop table if exists vrem_block;
create temporary table vrem_block 
		select 
t2.*
, t3.label 
from 
   (
    select 
        t1.regid
        , t1.app_n
        , date_format(to_date(t1.date_from), 'yyyyMM') as period
        , date_format(to_date(t1.date_to), 'yyyyMM') as period_x
        , t1.code
    FROM raw.mtsru_tdwh_smr_tdprd__uat_v_basefull_dapp_sblo t1 -- найти таблицу
    where t1.regid||'-'||t1.app_n in (select distinct a.regid||'-'||a.app_n from vrem_10 a)
    ) t2
left join 
   (
    select 
        c.start
        , c.label
    from
       (
        select distinct
            b.start
            , b.label
            , rank()Over(PARTITION BY b.start ORDER BY b.version_dt DESC) as rer1
        from raw.mtsru_sdwh_smr__mts_fmt_formats2_f67c b -- найти таблицу
        ) c
    where c.rer1 = 1
    ) t3
on t2.code = t3.start
where lower(t3.label) like '%заключит%'
    
)with data distributed randomly;
analyze vrem_block;


-- создаем vrem_block_1

drop table if exists vrem_block_1;
create temporary table vrem_block_1 
		select 
			a.*
			from
			(select
			t1.*
			, case when t1.period = t1.period_x then 1 else 0 end as mark_isk
			from vrem_block t1) a
			where a.mark_isk = 0
    
)with data distributed randomly;
analyze vrem_block_1;


-- создаем vrem_11

drop table if exists vrem_11;
create temporary table vrem_11 
		select 
			t1.period
			,t1.regid
			,t1.app_n
			,t1.phone_num_x2
			,t1.con_n_x2
			,t1.inn2
			,t1.kontr_name_x2
			,t1.activation_date_x2
			,t1.activat_date_x3
			,t1.activation_marker
			,t1.region_x2
			,t1.region_txt
			
			,t1.tariff_mgr
			,t1.tariff_plan_x4_prev
			,t1.tariff_plan_x2
			,t1.tariff_plan1_x4_prev
			,t1.tariff_plan1_x2
			
			,t1.tp_group_mgr
			,t1.tp_group_x4_prev
			,t1.tp_group_x2
			,t1.tp_group1_x4_prev
			,t1.tp_group1_x2
			
			,t1.tp_big_mgr
			,t1.tp_big_prev2
			,t1.tp_big
				
			,t1.categ_mgr
			,t1.con_typ_x4_prev
			,t1.con_typ_x2
			,t1.con_typ1_x4_prev
			,t1.con_typ1_x2
			
			,t1.segment_mgr
			,t1.segment_prev2
			,t1.segment
			,CASE 
				WHEN t1.segment <> t1.segment_prev2 AND (COALESCE(t1.segment_mgr, 0)) = 1
					THEN t1.segment_prev2||'-'||t1.segment
				END AS smena_segment
			
			,t1.fclc_prochie_mob2
			,t1.fclc_prochie_vas2
			,t1.fclc_golos2
			,t1.fclc_gprs2
			,t1.fclc_sms2
			,t1.fclc_vsr2
			,t1.fclc_vz_mg_mn2
			,t1.fclc_abon_plata2
			,t1.fclc_mob_total2
			,t1.fclc_mob_no_vas2
			,t1.fclc_total2
			,t1.fclc_aff_serv2
			,t1.activnost_mob_total
			,t1.activnost_mob_no_vas
			,t1.is_B2B
			,t1.ARPU_categ_mob_total
			,t1.ARPU_categ_mob_no_vas
			,t2.period as zak_block1
			,case 
				when COALESCE(t1.fclc_mob_total2, 0) - COALESCE(t1.fclc_aff_serv2, 0) = 0 and COALESCE(t1.fclc_mob_total2, 0) > 0 then 1
				else 0 
				end as metk_aff_ser
		from vrem_10 t1
		left join (select distinct a.regid, a.app_n, a.period from vrem_block_1 a) t2
			on t1.regid = t2.regid 
			and t1.app_n = t2.app_n 
			and t1.period = t2.period
		where t1.is_B2B = 'po_B2B'
    
)with data distributed randomly;
analyze vrem_11;


-- создаем vrem_12

drop table if exists vrem_12;
create temporary table vrem_12 
		SELECT
    b.* 
    , CASE
        when b.metk_zak_block1 is NULL 
            then first_value(b.metk_zak_block1) over (partition by b.regid, b.app_n, b.part_metk_block1 order by b.period)
        else b.metk_zak_block1
        end as mark_zak_block 
	, CASE
        when b.zak_block1 is NULL 
            then first_value(b.zak_block1) over (partition by b.regid, b.app_n, b.part_month_block1 order by b.period)
        else b.zak_block1
        end as month_zak_block 
from 
(select 
        a.*
        , sum(case when a.metk_zak_block1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_metk_block1
		, sum(case when a.zak_block1 is null then 0 else 1 end) 
			over (partition by a.regid, a.app_n order by a.period) part_month_block1
from 			
(select
	t1.period
	,t1.regid
	,t1.app_n
    ,t1.phone_num_x2
	,t1.con_n_x2
	,t1.inn2
	,case 
		when t1.con_n_x2 = t1.inn2 then 'con_n'
		else 'inn' end as is_con_inn
	,t1.kontr_name_x2
	,t1.activation_date_x2
	,t1.activat_date_x3
	,t1.activation_marker
	,t1.region_x2
	,t1.region_txt
	,t2.Reg_Week2 as Reg_Week2
	,t2.TR as TR
	
	,t1.tariff_mgr
	,t1.tariff_plan_x4_prev
	,t1.tariff_plan_x2
	,t1.tariff_plan1_x4_prev
	,t1.tariff_plan1_x2
	
	,t1.tp_group_mgr
	,t1.tp_group_x4_prev
	,t1.tp_group_x2
	,t1.tp_group1_x4_prev
	,t1.tp_group1_x2
	
	,t1.tp_big_mgr
	,t1.tp_big_prev2
	,t1.tp_big
		
	,t1.categ_mgr
	,t1.con_typ_x4_prev
	,t1.con_typ_x2
	,t1.con_typ1_x4_prev
	,t1.con_typ1_x2
	
	,t1.segment_mgr
	,t1.segment_prev2
	,t1.segment
	,t1.smena_segment
	
	,t1.fclc_prochie_mob2
    ,t1.fclc_prochie_vas2
    ,t1.fclc_golos2
    ,t1.fclc_gprs2
    ,t1.fclc_sms2
    ,t1.fclc_vsr2
    ,t1.fclc_vz_mg_mn2
    ,t1.fclc_abon_plata2
    ,t1.fclc_mob_total2
    ,t1.fclc_mob_no_vas2
	,t1.fclc_total2
	,t1.fclc_aff_serv2
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	,t1.is_B2B
	,t1.ARPU_categ_mob_total
    ,t1.ARPU_categ_mob_no_vas
	,t1.zak_block1
	,t1.metk_aff_ser
	,case 
		when t1.zak_block1 is not null then 1
		else null
		end as metk_zak_block1
from vrem_11 t1
left join schema_name.guide_tr t2
	on lower(t1.region_txt) = lower(t2.region)
) a
) b
    
)with data distributed randomly;
analyze vrem_12;



-- создаем vrem_13

drop table if exists vrem_13;
create temporary table vrem_13 
		select
	t1.period
	,t1.regid
	,t1.app_n
    ,t1.phone_num_x2 as phone_num
	,t1.con_n_x2 as con_n
	,t1.inn2 as inn
	,t1.is_con_inn
	,t1.kontr_name_x2 as kontr_name
	,t1.activation_date_x2 as abonent_activation_date
	,t1.activat_date_x3 as abonent_activation_month
	,t1.activation_marker
	,t1.region_x2 as region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	
	,t1.tariff_mgr
	,t1.tariff_plan_x4_prev as tariff_id_prev
	,t1.tariff_plan_x2 as tariff_id
	,t1.tariff_plan1_x4_prev as tariff_prev
	,t1.tariff_plan1_x2 as tariff
	
	,t1.tp_group_mgr
	,t1.tp_group_x4_prev as tp_group_id_prev
	,t1.tp_group_x2 as tp_group_id
	,t1.tp_group1_x4_prev as tp_group_prev
	,t1.tp_group1_x2 as tp_group
	
	,t1.tp_big_mgr
	,t1.tp_big_prev2 as tp_big_prev
	,t1.tp_big
		
	,t1.categ_mgr as m_categ_mgr
	,t1.con_typ_x4_prev as m_categ_id_prev
	,t1.con_typ_x2 as m_categ_id
	,t1.con_typ1_x4_prev as m_categ_prev
	,t1.con_typ1_x2 as m_categ
	
	,t1.segment_mgr
	,t1.segment_prev2 as segment_prev
	,t1.segment
	,t1.smena_segment as smena_segmenta
	
	,t1.fclc_prochie_mob2 as fclc_prochie_mob
    ,t1.fclc_prochie_vas2 as fclc_prochie_vas
    ,t1.fclc_golos2 as fclc_golos
    ,t1.fclc_gprs2 as fclc_gprs
    ,t1.fclc_sms2 as fclc_sms
    ,t1.fclc_vsr2 as fclc_vsr
    ,t1.fclc_vz_mg_mn2 as fclc_vz_mg_mn
    ,t1.fclc_abon_plata2 as fclc_abon_plata
    ,t1.fclc_mob_total2 as fclc_mob_total
    ,t1.fclc_mob_no_vas2 as fclc_mob_no_vas
	,t1.fclc_total2 as fclc_total
	,t1.fclc_aff_serv2 as fclc_aff_serv
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	,t1.ARPU_categ_mob_total as abonent_ARPU_categ_mob_total
    ,t1.ARPU_categ_mob_no_vas as abonent_ARPU_categ_mob_no_vas
	,t1.mark_zak_block
	,t1.month_zak_block
	,t1.metk_aff_ser
from vrem_12 t1
)with data distributed randomly;
analyze vrem_13;


-- создаем vrem_14

drop table if exists vrem_14;
create temporary table vrem_14 
select
	t1.period
	,t1.regid
	,t1.app_n
    ,t1.phone_num
	,t1.con_n
	,t1.inn
	,t1.is_con_inn
	,t1.kontr_name
	,t1.abonent_activation_date
	,t1.abonent_activation_month
	,t1.activation_marker
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	
	,t1.tariff_mgr
	,t1.tariff_id_prev
	,t1.tariff_id
	,t1.tariff_prev
	,t1.tariff
	
	,t1.tp_group_mgr
	,t1.tp_group_id_prev
	,t1.tp_group_id
	,t1.tp_group_prev
	,t1.tp_group
	
	,t1.tp_big_mgr
	,t1.tp_big_prev
	,t1.tp_big
		
	,t1.m_categ_mgr
	,t1.m_categ_id_prev
	,t1.m_categ_id
	,t1.m_categ_prev
	,t1.m_categ
	
	,t1.segment_mgr
	,t1.segment_prev
	,t1.segment
	,t1.smena_segmenta
	
	,COALESCE(t1.fclc_prochie_mob,0) as fclc_prochie_mob1
	,COALESCE(t1.fclc_prochie_vas,0) as fclc_prochie_vas1
	,COALESCE(t1.fclc_golos,0) as fclc_golos1
	,COALESCE(t1.fclc_gprs,0) as fclc_gprs1
	,COALESCE(t1.fclc_sms,0) as fclc_sms1
	,COALESCE(t1.fclc_vsr,0) as fclc_vsr1
	,COALESCE(t1.fclc_vz_mg_mn,0) as fclc_vz_mg_mn1
	,COALESCE(t1.fclc_abon_plata,0) as fclc_abon_plata1
	,COALESCE(t1.fclc_mob_total,0) as fclc_mob_total1
	,COALESCE(t1.fclc_mob_no_vas,0) as fclc_mob_no_vas1
	,COALESCE(t1.fclc_total,0) as fclc_total1
	,COALESCE(t1.fclc_aff_serv,0) as fclc_aff_serv1
	
	,CASE
		when COALESCE(t1.fclc_mob_total,0)=0 then NULL
		else t1.fclc_mob_total end as fclc_mob_total_x
	,CASE
		when COALESCE(t1.fclc_mob_no_vas,0)=0 then NULL
		else t1.fclc_mob_no_vas end as fclc_mob_no_vas_x
	,CASE
		when COALESCE(t1.fclc_total,0)=0 then NULL
		else t1.fclc_total end as fclc_total_x
	
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	
	,t1.abonent_ARPU_categ_mob_total
    ,t1.abonent_ARPU_categ_mob_no_vas
	
	,t1.mark_zak_block
	,t1.month_zak_block
	
	,t1.metk_aff_ser
	
--1 Приток моб тотал
	,case 
		when t1.period < t1.abonent_activation_month
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.tp_big<>'M2M/IOT' 
			and coalesce(t1.fclc_mob_total,0)>0 
		then 'Активный без активации'


		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.activation_marker=1 and tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Продажа'

		
		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big_prev='M2M/IOT' 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
		then 'Миграция из ТП IOT'
	
	
		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
			'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0
			and LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0
		then 'Миграция из др. бизнеса'

	
		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0 
		then 'Реактивация'

		
		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment_mgr=1 
		then 'Миграция в сегмент'
	
		
		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(t1.fclc_mob_total,0)>= 
				(LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*1.15)
		then 'Развитие'
		
		
		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(t1.fclc_mob_total,0)> 
				(LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*0.85)
			and COALESCE(t1.fclc_mob_total,0)< 
				(LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*1.15)
		then 'Стабильно платят'
	
		
		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг')
			AND COALESCE(t1.fclc_mob_total,0)<=
				(LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*0.85)
		then 'Оптимизация' 
	
	
	else 'Пусто'
	end as pritok
		
		
--2 Приток моб без вас
	,case 
		when t1.period < t1.abonent_activation_month
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.tp_big<>'M2M/IOT' 
			and coalesce(t1.fclc_mob_no_vas,0)>0 
		then 'Активный без активации'
	
	
		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.activation_marker=1 and tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Продажа'

		
		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big_prev='M2M/IOT' 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
		then 'Миграция из ТП IOT'
	
	
		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0
			and LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0
		then 'Миграция из др. бизнеса'

	
		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0 
		then 'Реактивация'

		
		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment_mgr=1 
		then 'Миграция в сегмент'
	
		
		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(t1.fclc_mob_no_vas,0)>= 
				(LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*1.15)
		then 'Развитие'
		
		
		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(t1.fclc_mob_no_vas,0)> 
				(LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*0.85)
			and COALESCE(t1.fclc_mob_no_vas,0)< 
				(LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*1.15)
		then 'Стабильно платят'
	
		
		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг')
			AND COALESCE(t1.fclc_mob_no_vas,0)<=
				(LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*0.85)
		then 'Оптимизация' 
	
	
	else 'Пусто'
	end as pritok_no_vas		
		
		
--3 Приток моб тотал Общ		
	,case
		when t1.period < t1.abonent_activation_month
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.tp_big<>'M2M/IOT' 
			and coalesce(t1.fclc_mob_total,0)>0 
		then 'Активный без активации'
	
	
		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.activation_marker=1 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Продажа'


		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big_prev='M2M/IOT' 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
		then 'Миграция из ТП IOT'


		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг')  
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0
			and LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0
		then 'Миграция из др. бизнеса'


		when coalesce(fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0 
		then 'Реактивация'


		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment_prev NOT IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0
			and LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0
		then 'Миграция в B2B'


		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(t1.fclc_mob_total,0)>= 
				(LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*1.15)
		then 'Развитие'


		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(t1.fclc_mob_total,0)> 
				(LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*0.85)
			and COALESCE(t1.fclc_mob_total,0)< 
				(LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*1.15)
		then 'Стабильно платят'


		when coalesce(t1.fclc_mob_total,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(t1.fclc_mob_total,0)<=
				(LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*0.85)
		then 'Оптимизация' 


	else 'Пусто'
	end as pritok_all
	
	
--4 Приток моб без вас Общ
	,case
		when t1.period < t1.abonent_activation_month
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.tp_big<>'M2M/IOT' 
			and coalesce(t1.fclc_mob_no_vas,0)>0 
		then 'Активный без активации'
	
	
		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.activation_marker=1 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Продажа'


		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big_prev='M2M/IOT' 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
		then 'Миграция из ТП IOT'


		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг')  
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0
			and LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0
		then 'Миграция из др. бизнеса'


		when coalesce(fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0 
		then 'Реактивация'


		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment_prev NOT IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(t1.fclc_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0
			and LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0
		then 'Миграция в B2B'


		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(t1.fclc_mob_no_vas,0)>= 
				(LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*1.15)
		then 'Развитие'


		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(t1.fclc_mob_no_vas,0)> 
				(LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*0.85)
			and COALESCE(t1.fclc_mob_no_vas,0)< 
				(LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*1.15)
		then 'Стабильно платят'


		when coalesce(t1.fclc_mob_no_vas,0)>0 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(t1.fclc_mob_no_vas,0)<=
				(LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)*0.85)
		then 'Оптимизация' 


	else 'Пусто'
	end as pritok_all_no_vas	
	

--5 Отток моб тотал
	,CASE 
		when LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0 
			and coalesce(t1.fclc_mob_total, 0)=0 
		then 'Ранее перешёл в отток'

	
		when LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and coalesce(t1.fclc_mob_total, 0)=0 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(t1.period) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<t1.abonent_activation_month
		then 'Отток с активацией'
	
	
		when coalesce(t1.fclc_mob_total,0)=0 
			and LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and coalesce(t1.fclc_total,0)=0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
			'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Отток'


		when LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0
			and coalesce(t1.fclc_mob_total,0)=0            
			and coalesce(t1.fclc_total,0)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в др. бизнес'


		when LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.tp_big='M2M/IOT' 
			and COALESCE(t1.activation_marker,0) <> 1
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в ТП IOT'


		when LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment_mgr=1 
		then 'Миграция из сегмента'


	else 'Пусто'
	end as ottok
	
	
--6 Отток моб без вас	
	,CASE 
		when LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0 
			and coalesce(t1.fclc_mob_no_vas, 0)=0 
		then 'Ранее перешёл в отток'


		when LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and coalesce(t1.fclc_mob_no_vas, 0)=0 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(t1.period) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<t1.abonent_activation_month
		then 'Отток с активацией'


		when coalesce(t1.fclc_mob_no_vas,0)=0 
			and LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and coalesce(t1.fclc_total,0)=0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Отток'


		when LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and coalesce(t1.fclc_mob_no_vas,0)=0
			and coalesce(t1.fclc_total,0)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в др. бизнес'


		when LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.tp_big='M2M/IOT'
			and COALESCE(t1.activation_marker,0) <> 1
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в ТП IOT'


		when LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment_mgr=1 
		then 'Миграция из сегмента'


	else 'Пусто'
	end as ottok_no_vas	
	
	
--7 Отток моб тотал Общ
	,CASE 
		when LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0 
			and coalesce(t1.fclc_mob_total,0)=0 
		then 'Ранее перешёл в отток'

	
		when LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and coalesce(t1.fclc_mob_total, 0)=0 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(t1.period) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<t1.abonent_activation_month
		then 'Отток с активацией'
	
	
		when coalesce(t1.fclc_mob_total,0)=0 
			and LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and coalesce(t1.fclc_total, 0)=0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Отток'


		when LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0
			and coalesce(t1.fclc_mob_total,0)=0            
			and coalesce(t1.fclc_total,0)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в др. бизнес'


		when LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.tp_big='M2M/IOT' 
			and COALESCE(t1.activation_marker,0) <> 1
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в ТП IOT'


		when LAG(COALESCE(t1.fclc_mob_total,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment NOT IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция из B2B'
	
	
	else 'Пусто' 
	end as ottok_all


--8 Отток моб без вас Общ
	,CASE 
		when LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)=0 
			and coalesce(t1.fclc_mob_no_vas,0)=0 
		then 'Ранее перешёл в отток'

	
		when LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and coalesce(t1.fclc_mob_no_vas, 0)=0 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(t1.period) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<t1.abonent_activation_month
		then 'Отток с активацией'
	
	
		when coalesce(t1.fclc_mob_no_vas,0)=0 
			and LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and coalesce(t1.fclc_total, 0)=0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Отток'


		when LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and coalesce(t1.fclc_mob_no_vas,0)=0
			and coalesce(t1.fclc_total,0)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.tp_big<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в др. бизнес'



		when LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.tp_big='M2M/IOT' 
			and COALESCE(t1.activation_marker,0) <> 1
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в ТП IOT'


		When LAG(COALESCE(t1.fclc_mob_no_vas,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period)<>0 
			and t1.tp_big_prev<>'M2M/IOT' 
			and t1.segment_prev IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and t1.segment NOT IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция из B2B'
	
	else 'Пусто' 
	end as ottok_all_no_vas
	
from vrem_13 t1

		
)with data distributed randomly;
analyze vrem_14;

-- создаем vrem_15

drop table if exists vrem_15;
create temporary table vrem_15 
		select
	t1.regid
	,t1.app_n
	,t1.tariff_id
	,t1.tariff
	,t1.tp_group_id
	,t1.tp_group
	,t1.tp_big
	,t1.m_categ_id
	,t1.m_categ
	,t1.segment
	,t1.inn
    ,t1.kontr_name
FROM (select a.*, row_number() OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period desc) as rer from vrem_14 a) as t1
where t1.rer = 1 
)with data distributed randomly;
analyze vrem_15;


-- создаем vrem_16

drop table if exists vrem_16;
create temporary table vrem_16 
		SELECT
	b.period
    ,b.inn
	,count(*) as rew
from
   (
	select distinct
		t1.period
        ,t1.inn
		, t1.tp_big
	from analyticsb2b_sb.table_vr_mob_vrem_14 t1
	where t1.inn||'-'||t1.period in (select distinct a.inn||'-'||a.period from vrem_14 a where a.tp_big = 'M2M/IOT')
	) b
group by 
	b.period
    ,b.inn
having 
	count(*) = 1
)with data distributed randomly;
analyze vrem_16;


-- создаем vrem_17

drop table if exists vrem_17;
create temporary table vrem_17 
		select
	t1.period
	, case 
		when t1.period = 202101 then '2021-01-01'
		when t1.period = 202102 then '2021-02-01'
		when t1.period = 202103 then '2021-03-01'
		when t1.period = 202104 then '2021-04-01'
		when t1.period = 202105 then '2021-05-01'
		when t1.period = 202106 then '2021-06-01'
		when t1.period = 202107 then '2021-07-01'
		when t1.period = 202108 then '2021-08-01'
		when t1.period = 202109 then '2021-09-01'
		when t1.period = 202110 then '2021-10-01'
		when t1.period = 202111 then '2021-11-01'
		when t1.period = 202112 then '2021-12-01'
		when t1.period = 202201 then '2022-01-01'
		when t1.period = 202202 then '2022-02-01'
		when t1.period = 202203 then '2022-03-01'
		when t1.period = 202204 then '2022-04-01'
		when t1.period = 202205 then '2022-05-01'
		when t1.period = 202206 then '2022-06-01'
		when t1.period = 202207 then '2022-07-01'
		when t1.period = 202208 then '2022-08-01'
		when t1.period = 202209 then '2022-09-01'
		when t1.period = 202210 then '2022-10-01'
		when t1.period = 202211 then '2022-11-01'
		when t1.period = 202212 then '2022-12-01'
		when t1.period = 202301 then '2023-01-01'
		when t1.period = 202302 then '2023-02-01'
		when t1.period = 202303 then '2023-03-01'
		when t1.period = 202304 then '2023-04-01'
		when t1.period = 202305 then '2023-05-01'
		when t1.period = 202306 then '2023-06-01'
		when t1.period = 202307 then '2023-07-01'
		when t1.period = 202308 then '2023-08-01'
		when t1.period = 202309 then '2023-09-01'
		when t1.period = 202310 then '2023-10-01'
		when t1.period = 202311 then '2023-11-01'
		when t1.period = 202312 then '2023-12-01'
        when t1.period = 202401 then '2024-01-01'
        when t1.period = 202402 then '2024-02-01'
        when t1.period = 202403 then '2024-03-01'
		when t1.period = 202404 then '2024-04-01'
		when t1.period = 202405 then '2024-05-01'
		when t1.period = 202406 then '2024-06-01'
		when t1.period = 202407 then '2024-07-01'
		when t1.period = 202408 then '2024-08-01'
		when t1.period = 202409 then '2024-09-01'
		when t1.period = 202410 then '2024-10-01'
		when t1.period = 202411 then '2024-11-01'
		when t1.period = 202412 then '2024-12-01'
		end as period_date
	,t1.regid
	,t1.app_n
    ,t1.phone_num
	,t1.con_n
	,t1.inn
	,t1.is_con_inn
	,t1.kontr_name
	,t1.abonent_activation_date
	,t1.abonent_activation_month
	,t1.activation_marker
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	
	,t1.tariff_mgr
	,t1.tariff_id_prev
	,t1.tariff_id
	,t1.tariff_prev
	,t1.tariff
	
	,t1.tp_group_mgr
	,t1.tp_group_id_prev
	,t1.tp_group_id
	,t1.tp_group_prev
	,t1.tp_group
	
	,t1.tp_big_mgr
	,t1.tp_big_prev
	,t1.tp_big
		
	,t1.m_categ_mgr
	,t1.m_categ_id_prev
	,t1.m_categ_id
	,t1.m_categ_prev
	,t1.m_categ
	
	,t1.segment_mgr
	,t1.segment_prev
	,t1.segment
	,t1.smena_segmenta
	
	,t1.fclc_prochie_mob1
	,t1.fclc_prochie_vas1
	,t1.fclc_golos1
	,t1.fclc_gprs1
	,t1.fclc_sms1
	,t1.fclc_vsr1
	,t1.fclc_vz_mg_mn1
	,t1.fclc_abon_plata1
	,t1.fclc_mob_total1
	,t1.fclc_mob_no_vas1
	,t1.fclc_total1
	,t1.fclc_aff_serv1
	
	,LAG(COALESCE(t1.fclc_mob_total1,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_mob_total1_prev
	,LAG(COALESCE(t1.fclc_mob_no_vas1,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_mob_no_vas1_prev
	,LAG(COALESCE(t1.fclc_total1,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_total1_prev
	
	,t1.fclc_mob_total_x
	,t1.fclc_mob_no_vas_x
	,t1.fclc_total_x
	
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	
	,t1.abonent_ARPU_categ_mob_total
    ,t1.abonent_ARPU_categ_mob_no_vas
	
	,t1.mark_zak_block
	,t1.month_zak_block
	
	,t1.metk_aff_ser
	
	,t1.pritok as pritok_mob_total
	,t1.pritok_no_vas as pritok_mob_no_vas
	,t1.pritok_all as pritok_mob_total_all
	,t1.pritok_all_no_vas as pritok_mob_no_vas_all
	,t1.ottok as ottok_mob_total
	,t1.ottok_no_vas as ottok_mob_no_vas
	,t1.ottok_all as ottok_mob_total_all
	,t1.ottok_all_no_vas as ottok_mob_no_vas_all
	
	,AVG(t1.fclc_mob_total_x) 
		OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) 
		as avg_do_ottoka_abonenta_mob_total
	,AVG(t1.fclc_mob_no_vas_x) 
		OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) 
		as avg_do_ottoka_abonenta_mob_no_vas
	
	,t2.tariff_id as tariff_id_as_is
	,t2.tariff as tariff_as_is
	,t2.tp_group_id as tp_group_id_as_is
	,t2.tp_group as tp_group_as_is
	,t2.tp_big as tp_big_as_is
	,t2.m_categ_id as m_categ_id_as_is
	,t2.m_categ as m_categ_as_is
	,t2.segment as segment_as_is
	,t2.inn as inn_as_is
    ,t2.kontr_name as kontr_name_as_is
	
	,case 
		when t3.inn is null then 0
		else 1
		end as mark_inn_m2m_iot
	
from vrem_14 t1
left join vrem_15 t2
	on t1.regid = t2.regid
	and t1.app_n = t2.app_n
left join vrem_16 t3
	on t1.inn = t3.inn
	and t1.period = t3.period
)with data distributed randomly;
analyze vrem_17;


-- создаем vrem_18

drop table if exists vrem_18;
create temporary table vrem_18 
		select
	t1.period
	,t1.period_date
	,t1.regid
	,t1.app_n
    ,t1.phone_num
	,t1.con_n
	,t1.inn
	,t1.is_con_inn
	,t1.kontr_name
    ,t1.kontr_name_as_is
    
	,t1.abonent_activation_date
	,t1.abonent_activation_month
	,t1.activation_marker
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	
	,t1.tariff_mgr
	,t1.tariff_id_prev
	,t1.tariff_id
	,t1.tariff_prev
	,t1.tariff
	
	,t1.tp_group_mgr
	,t1.tp_group_id_prev
	,t1.tp_group_id
	,t1.tp_group_prev
	,t1.tp_group
	
	,t1.tp_big_mgr
	,t1.tp_big_prev
	,t1.tp_big
		
	,t1.m_categ_mgr
	,t1.m_categ_id_prev
	,t1.m_categ_id
	,t1.m_categ_prev
	,t1.m_categ
	
	,t1.segment_mgr
	,t1.segment_prev
	,t1.segment
	,t1.smena_segmenta
	
	,t1.fclc_prochie_mob1
	,t1.fclc_prochie_vas1
	,t1.fclc_golos1
	,t1.fclc_gprs1
	,t1.fclc_sms1
	,t1.fclc_vsr1
	,t1.fclc_vz_mg_mn1
	,t1.fclc_abon_plata1
	,t1.fclc_mob_total1
	,t1.fclc_mob_no_vas1
	,t1.fclc_total1
	,t1.fclc_aff_serv1
	
	,t1.fclc_mob_total1_prev
	,t1.fclc_mob_no_vas1_prev
	,t1.fclc_total1_prev
	
	,t1.fclc_mob_total_x
	,t1.fclc_mob_no_vas_x
	,t1.fclc_total_x
	
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	
	,t1.abonent_ARPU_categ_mob_total
    ,t1.abonent_ARPU_categ_mob_no_vas
	
	,t1.mark_zak_block
	,t1.month_zak_block
	
	,t1.metk_aff_ser
	
	,t1.pritok_mob_total
	,t1.pritok_mob_no_vas
	,t1.pritok_mob_total_all
	,t1.pritok_mob_no_vas_all
	,t1.ottok_mob_total
	,t1.ottok_mob_no_vas
	,t1.ottok_mob_total_all
	,t1.ottok_mob_no_vas_all
	
	,t1.avg_do_ottoka_abonenta_mob_total
	,t1.avg_do_ottoka_abonenta_mob_no_vas
	,case 
		when t1.ottok_mob_total in ('Отток', 'Отток с активацией') then t1.avg_do_ottoka_abonenta_mob_total
		else null end as avg_do_ottoka_abonenta_mob_total1
	,case 
		when t1.ottok_mob_no_vas in ('Отток', 'Отток с активацией') then t1.avg_do_ottoka_abonenta_mob_no_vas
		else null end as avg_do_ottoka_abonenta_mob_no_vas1
	
	,t1.fclc_mob_total1 - t1.fclc_mob_total1_prev as sum_status_mob_total
	,t1.fclc_mob_no_vas1 - t1.fclc_mob_no_vas1_prev as sum_status_mob_no_vas
	
	,t1.tariff_id_as_is
	,t1.tariff_as_is
	,t1.tp_group_id_as_is
	,t1.tp_group_as_is
	,t1.tp_big_as_is
	,t1.m_categ_id_as_is
	,t1.m_categ_as_is
	,t1.segment_as_is
	,t1.inn_as_is
	
	,t1.mark_inn_m2m_iot
	
from vrem_17 t1
)with data distributed randomly;
analyze vrem_18;


-- создаем vrem_19

drop table if exists vrem_19;
create temporary table vrem_19 
		SELECT DISTINCT 
    regid
    , app_n
    , LAST_VALUE(period) OVER (PARTITION BY regid, app_n order by period) as last_ottok_mob_total
FROM vrem_18
WHERE ottok_mob_total in ('Отток', 'Отток с активацией')
)with data distributed randomly;
analyze vrem_19;


-- создаем vrem_20

drop table if exists vrem_20;
create temporary table vrem_20 
		SELECT DISTINCT 
    regid
    , app_n
    , LAST_VALUE(period) OVER (PARTITION BY regid, app_n order by period) as last_ottok_mob_no_vas
FROM vrem_18
WHERE ottok_mob_no_vas in ('Отток', 'Отток с активацией')
)with data distributed randomly;
analyze vrem_20;


-- создаем vrem_21

drop table if exists vrem_21;
create temporary table vrem_21 
		SELECT 
      t1.*
    , t2.last_ottok_mob_total
    , t3.last_ottok_mob_no_vas
FROM vrem_18 as t1
LEFT JOIN vrem_19 as t2
    ON t1.period = t2.last_ottok_mob_total 
    and t1.regid = t2.regid 
    and t1.app_n = t2.app_n
LEFT JOIN vrem_20 as t3
    ON t1.period = t3.last_ottok_mob_no_vas 
    and t1.regid = t3.regid 
    and t1.app_n = t3.app_n
)with data distributed randomly;
analyze vrem_21;


-- создаем vrem_22

drop table if exists vrem_22;
create temporary table vrem_22 
		SELECT 
    t1.*
    , CASE 
        WHEN t1.ottok_mob_total = 'Ранее перешёл в отток' 
            then (last_value(t1.last_ottok_mob_total, true) over (partition by t1.regid, t1.app_n order by t1.period ROWS UNBOUNDED PRECEDING))
        WHEN t1.ottok_mob_total in ('Отток', 'Отток с активацией') 
            then t1.last_ottok_mob_total 
        end as last_ottok_mob_total1
    , CASE 
        WHEN t1.ottok_mob_no_vas = 'Ранее перешёл в отток' 
            then (last_value(t1.last_ottok_mob_no_vas, true) over (partition by t1.regid, t1.app_n order by t1.period ROWS UNBOUNDED PRECEDING))
        WHEN t1.ottok_mob_no_vas in ('Отток', 'Отток с активацией') 
            then t1.last_ottok_mob_no_vas 
        end as last_ottok_mob_no_vas1
FROM vrem_21 as t1
)with data distributed randomly;
analyze vrem_22;


-- создаем vrem_23

drop table if exists vrem_23;
create temporary table vrem_23 
		SELECT 
    t1.*
    , CASE 
        WHEN t1.ottok_mob_total in ('Отток', 'Отток с активацией')
            and t1.period = t1.last_ottok_mob_total1 
            then t1.avg_do_ottoka_abonenta_mob_total1
        WHEN t1.ottok_mob_total = 'Ранее перешёл в отток' 
            then (last_value(t1.avg_do_ottoka_abonenta_mob_total1, true) 
				over (partition by t1.regid, t1.app_n, t1.last_ottok_mob_total1 
					order by t1.period ROWS UNBOUNDED PRECEDING))
        END AS rl_po_ottoku_mob_total 
FROM vrem_22 as t1  
)with data distributed randomly;
analyze vrem_23;


-- создаем vrem_24

drop table if exists vrem_24;
create temporary table vrem_24 
		SELECT 
    t1.*
    , CASE 
        WHEN t1.ottok_mob_no_vas in ('Отток', 'Отток с активацией')
            and t1.period = t1.last_ottok_mob_no_vas1 
            then t1.avg_do_ottoka_abonenta_mob_no_vas1
        WHEN t1.ottok_mob_no_vas = 'Ранее перешёл в отток'
            then (last_value(t1.avg_do_ottoka_abonenta_mob_no_vas1, true) 
				over (partition by t1.regid, t1.app_n, t1.last_ottok_mob_no_vas1 
					order by t1.period ROWS UNBOUNDED PRECEDING))
        END AS rl_po_ottoku_mob_no_vas 
FROM vrem_23 as t1  
)with data distributed randomly;
analyze vrem_24;


-- создаем vrem_25

drop table if exists vrem_25
create temporary table vrem_25 
		select
	t1.period
	,t1.period_date
	,t1.regid
	,t1.app_n
    ,t1.phone_num
	,t1.con_n
	,t1.inn
	,t1.is_con_inn
	,t1.kontr_name
	,t1.abonent_activation_date
	,t1.abonent_activation_month
	,t1.activation_marker
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	
	,t1.tariff_mgr
	,t1.tariff_id_prev
	,t1.tariff_id
	,t1.tariff_prev
	,t1.tariff
	
	,t1.tp_group_mgr
	,t1.tp_group_id_prev
	,t1.tp_group_id
	,t1.tp_group_prev
	,t1.tp_group
	
	,t1.tp_big_mgr
	,t1.tp_big_prev
	,t1.tp_big
		
	,t1.m_categ_mgr
	,t1.m_categ_id_prev
	,t1.m_categ_id
	,t1.m_categ_prev
	,t1.m_categ
	
	,t1.segment_mgr
	,t1.segment_prev
	,t1.segment
	,t1.smena_segmenta
	
	,t1.fclc_prochie_mob1
	,t1.fclc_prochie_vas1
	,t1.fclc_golos1
	,t1.fclc_gprs1
	,t1.fclc_sms1
	,t1.fclc_vsr1
	,t1.fclc_vz_mg_mn1
	,t1.fclc_abon_plata1
	,t1.fclc_mob_total1
	,t1.fclc_mob_no_vas1
	,t1.fclc_total1
	,t1.fclc_aff_serv1
	
	,t1.fclc_mob_total1_prev
	,t1.fclc_mob_no_vas1_prev
	,t1.fclc_total1_prev
	
	,t1.fclc_mob_total_x
	,t1.fclc_mob_no_vas_x
	,t1.fclc_total_x
	
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	
	,t1.abonent_ARPU_categ_mob_total
    ,t1.abonent_ARPU_categ_mob_no_vas
	,case 
		when coalesce(t1.avg_do_ottoka_abonenta_mob_total, 0) = 0 
			then '0'
		when coalesce(t1.avg_do_ottoka_abonenta_mob_total, 0) > 0
			and coalesce(t1.avg_do_ottoka_abonenta_mob_total, 0) < 50 
			then '0_50'
		when coalesce(t1.avg_do_ottoka_abonenta_mob_total, 0) >= 50
			and coalesce(t1.avg_do_ottoka_abonenta_mob_total, 0) < 100 
			then '50_100'
		when coalesce(t1.avg_do_ottoka_abonenta_mob_total, 0) >= 100
			and coalesce(t1.avg_do_ottoka_abonenta_mob_total, 0) < 250 
			then '100_250'
		when coalesce(t1.avg_do_ottoka_abonenta_mob_total, 0) >= 250
			and coalesce(t1.avg_do_ottoka_abonenta_mob_total, 0) < 500 
			then '250_500'
		when coalesce(t1.avg_do_ottoka_abonenta_mob_total, 0) >= 500 
			then '500_i_bolshe'
		end as abonent_arpu_categ_ottoka_mob_total
    ,case 
		when coalesce(t1.avg_do_ottoka_abonenta_mob_no_vas, 0) = 0 
			then '0'
		when coalesce(t1.avg_do_ottoka_abonenta_mob_no_vas, 0) > 0
			and coalesce(t1.avg_do_ottoka_abonenta_mob_no_vas, 0) < 50 
			then '0_50'
		when coalesce(t1.avg_do_ottoka_abonenta_mob_no_vas, 0) >= 50
			and coalesce(t1.avg_do_ottoka_abonenta_mob_no_vas, 0) < 100 
			then '50_100'
		when coalesce(t1.avg_do_ottoka_abonenta_mob_no_vas, 0) >= 100
			and coalesce(t1.avg_do_ottoka_abonenta_mob_no_vas, 0) < 250 
			then '100_250'
		when coalesce(t1.avg_do_ottoka_abonenta_mob_no_vas, 0) >= 250
			and coalesce(t1.avg_do_ottoka_abonenta_mob_no_vas, 0) < 500 
			then '250_500'
		when coalesce(t1.avg_do_ottoka_abonenta_mob_no_vas, 0) >= 500 
			then '500_i_bolshe'
		end as abonent_arpu_categ_ottoka_mob_no_vas
	
	,t1.mark_zak_block
	,t1.month_zak_block
	
	,t1.metk_aff_ser
	
	,t1.pritok_mob_total
	,t1.pritok_mob_no_vas
	,t1.pritok_mob_total_all
	,t1.pritok_mob_no_vas_all
	,t1.ottok_mob_total
	,t1.ottok_mob_no_vas
	,t1.ottok_mob_total_all
	,t1.ottok_mob_no_vas_all
	
	,t1.avg_do_ottoka_abonenta_mob_total
	,t1.avg_do_ottoka_abonenta_mob_no_vas
	,t1.rl_po_ottoku_mob_total
	,t1.rl_po_ottoku_mob_no_vas
	,t1.last_ottok_mob_total1 as last_date_ottok_mob_total
	,t1.last_ottok_mob_no_vas1 as last_date_ottok_mob_no_vas
	
	,case 
		when t1.fclc_mob_total1_prev = 0 then 100
		else (t1.fclc_mob_total1/t1.fclc_mob_total1_prev - 1) * 100
		end as proc_status_mob_total
	,case 
		when t1.fclc_mob_no_vas1_prev = 0 then 100
		else (t1.fclc_mob_no_vas1/t1.fclc_mob_no_vas1_prev - 1) * 100
		end as proc_status_mob_no_vas
	
	,t1.sum_status_mob_total
	,t1.sum_status_mob_no_vas
	
	,t1.tariff_id_as_is
	,t1.tariff_as_is
	,t1.tp_group_id_as_is
	,t1.tp_group_as_is
	,t1.tp_big_as_is
	,t1.m_categ_id_as_is
	,t1.m_categ_as_is
	,t1.segment_as_is
	,t1.inn_as_is
    ,t1.kontr_name_as_is
	
	,t1.mark_inn_m2m_iot
	
from vrem_24 t1  
)with data distributed randomly;
analyze vrem_25;

-- создаем vrem_26

drop table if exists vrem_26;
create temporary table vrem_26 
		select
	t1.period
	,t1.period_date
	,t1.regid
	,t1.app_n
	,t1.phone_num
	,t1.con_n
	,t1.inn
	,t1.is_con_inn
	,t1.kontr_name
	,t1.abonent_activation_date
	,t1.abonent_activation_month
	,t1.activation_marker
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	
	,t1.tariff_mgr
	,t1.tariff_id_prev
	,t1.tariff_id
	,t1.tariff_prev
	,t1.tariff
	
	,t1.tp_group_mgr
	,t1.tp_group_id_prev
	,t1.tp_group_id
	,t1.tp_group_prev
	,t1.tp_group
	
	,t1.tp_big_mgr
	,t1.tp_big_prev
	,t1.tp_big
		
	,t1.m_categ_mgr
	,t1.m_categ_id_prev
	,t1.m_categ_id
	,t1.m_categ_prev
	,t1.m_categ
	
	,t1.segment_mgr
	,t1.segment_prev
	,t1.segment
	,t1.smena_segmenta
	
	,t1.fclc_prochie_mob1 as fclc_prochie_mob
	,t1.fclc_prochie_vas1 as fclc_prochie_vas
	,t1.fclc_golos1 as fclc_golos
	,t1.fclc_gprs1 as fclc_gprs
	,t1.fclc_sms1 as fclc_sms
	,t1.fclc_vsr1 as fclc_vsr
	,t1.fclc_vz_mg_mn1 as fclc_vz_mg_mn
	,t1.fclc_abon_plata1 as fclc_abon_plata
	,t1.fclc_mob_total1 as fclc_mob_total
	,t1.fclc_mob_no_vas1 as fclc_mob_no_vas
	,t1.fclc_total1 as fclc_total
	,t1.fclc_aff_serv1 as fclc_aff_serv
	
	,t1.fclc_mob_total1_prev as fclc_mob_total_prev
	,t1.fclc_mob_no_vas1_prev as fclc_mob_no_vas_prev
	,t1.fclc_total1_prev as fclc_total_prev
	
	,t1.fclc_mob_total_x
	,t1.fclc_mob_no_vas_x
	,t1.fclc_total_x
	
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	
	,t1.abonent_ARPU_categ_mob_total as ARPU_abonent_categ_mob_total
    ,t1.abonent_ARPU_categ_mob_no_vas as ARPU_abonent_categ_mob_no_vas
	,t1.abonent_arpu_categ_ottoka_mob_total as ARPU_abonent_categ_ottoka_mob_total
    ,t1.abonent_arpu_categ_ottoka_mob_no_vas as ARPU_abonent_categ_ottoka_mob_no_vas
	
	,t1.mark_zak_block
	,t1.month_zak_block
	
	,t1.metk_aff_ser
	
	,t1.pritok_mob_total
	,t1.pritok_mob_no_vas
	,t1.pritok_mob_total_all
	,t1.pritok_mob_no_vas_all
	,t1.ottok_mob_total
	,t1.ottok_mob_no_vas
	,t1.ottok_mob_total_all
	,t1.ottok_mob_no_vas_all
	
	,t1.avg_do_ottoka_abonenta_mob_total
	,t1.avg_do_ottoka_abonenta_mob_no_vas
	,t1.rl_po_ottoku_mob_total as rl_po_ottoku_abonenta_mob_total
	,t1.rl_po_ottoku_mob_no_vas as rl_po_ottoku_abonenta_mob_no_vas
	,t1.last_date_ottok_mob_total as ottok_last_month_mob_total 
	,t1.last_date_ottok_mob_no_vas as ottok_last_month_mob_no_vas 
	
	,t1.proc_status_mob_total
	,t1.proc_status_mob_no_vas
	,case 
		when t1.proc_status_mob_total = 0 then '0'
		when t1.proc_status_mob_total < 0 and t1.proc_status_mob_total >= -5 then '<0_>=-5%'
		when t1.proc_status_mob_total < -5 and t1.proc_status_mob_total >= -10 then '<-5_>=-10%'
		when t1.proc_status_mob_total < -10 and t1.proc_status_mob_total >= -15 then '<-10_>=-15%'
		when t1.proc_status_mob_total < -15 and t1.proc_status_mob_total >= -20 then '<-15_>=-20%'
		when t1.proc_status_mob_total < -20 and t1.proc_status_mob_total >= -25 then '<-20_>=-25%'
		when t1.proc_status_mob_total < -25 and t1.proc_status_mob_total >= -30 then '<-25_>=-30%'
		when t1.proc_status_mob_total < -30 and t1.proc_status_mob_total >= -35 then '<-30_>=-35%'
		when t1.proc_status_mob_total < -35 and t1.proc_status_mob_total >= -40 then '<-35_>=-40%'
		when t1.proc_status_mob_total < -40 and t1.proc_status_mob_total >= -45 then '<-40_>=-45%'
		when t1.proc_status_mob_total < -45 and t1.proc_status_mob_total >= -50 then '<-45_>=-50%'
		when t1.proc_status_mob_total < -50 and t1.proc_status_mob_total >= -55 then '<-50_>=-55%'
		when t1.proc_status_mob_total < -55 and t1.proc_status_mob_total >= -60 then '<-55_>=-60%'
		when t1.proc_status_mob_total < -60 and t1.proc_status_mob_total >= -65 then '<-60_>=-65%'
		when t1.proc_status_mob_total < -65 and t1.proc_status_mob_total >= -70 then '<-65_>=-70%'
		when t1.proc_status_mob_total < -70 and t1.proc_status_mob_total >= -75 then '<-70_>=-75%'
		when t1.proc_status_mob_total < -75 and t1.proc_status_mob_total >= -80 then '<-75_>=-80%'
		when t1.proc_status_mob_total < -80 and t1.proc_status_mob_total >= -85 then '<-80_>=-85%'
		when t1.proc_status_mob_total < -85 and t1.proc_status_mob_total >= -90 then '<-85_>=-90%'
		when t1.proc_status_mob_total < -90 and t1.proc_status_mob_total >= -95 then '<-90_>=-95%'
		when t1.proc_status_mob_total < -95 and t1.proc_status_mob_total >= -100 then '<-95_>=-100%'
		
		when t1.proc_status_mob_total > 0 and t1.proc_status_mob_total <= 5 then '>0_<=5%'
		when t1.proc_status_mob_total > 5 and t1.proc_status_mob_total <= 10 then '>5_<=10%'
		when t1.proc_status_mob_total > 10 and t1.proc_status_mob_total <= 15 then '>10_<=15%'
		when t1.proc_status_mob_total > 15 and t1.proc_status_mob_total <= 20 then '>15_<=20%'
		when t1.proc_status_mob_total > 20 and t1.proc_status_mob_total <= 25 then '>20_<=25%'
		when t1.proc_status_mob_total > 25 and t1.proc_status_mob_total <= 30 then '>25_<=30%'
		when t1.proc_status_mob_total > 30 and t1.proc_status_mob_total <= 35 then '>30_<=35%'
		when t1.proc_status_mob_total > 35 and t1.proc_status_mob_total <= 40 then '>35_<=40%'
		when t1.proc_status_mob_total > 40 and t1.proc_status_mob_total <= 45 then '>40_<=45%'
		when t1.proc_status_mob_total > 45 and t1.proc_status_mob_total <= 50 then '>45_<=50%'
		when t1.proc_status_mob_total > 50 and t1.proc_status_mob_total <= 55 then '>50_<=55%'
		when t1.proc_status_mob_total > 55 and t1.proc_status_mob_total <= 60 then '>55_<=60%'
		when t1.proc_status_mob_total > 60 and t1.proc_status_mob_total <= 65 then '>60_<=65%'
		when t1.proc_status_mob_total > 65 and t1.proc_status_mob_total <= 70 then '>65_<=70%'
		when t1.proc_status_mob_total > 70 and t1.proc_status_mob_total <= 75 then '>70_<=75%'
		when t1.proc_status_mob_total > 75 and t1.proc_status_mob_total <= 80 then '>75_<=80%'
		when t1.proc_status_mob_total > 80 and t1.proc_status_mob_total <= 85 then '>80_<=85%'
		when t1.proc_status_mob_total > 85 and t1.proc_status_mob_total <= 90 then '>85_<=90%'
		when t1.proc_status_mob_total > 90 and t1.proc_status_mob_total <= 95 then '>90_<=95%'
		when t1.proc_status_mob_total > 95 and t1.proc_status_mob_total <= 100 then '>95_<=100%'
		when t1.proc_status_mob_total > 100 then '>100%'
		
		else '0' end as categ_proc_status_mob_total
	,case 
		when t1.proc_status_mob_no_vas = 0 then '0'
		when t1.proc_status_mob_no_vas < 0 and t1.proc_status_mob_no_vas >= -5 then '<0_>=-5%'
		when t1.proc_status_mob_no_vas < -5 and t1.proc_status_mob_no_vas >= -10 then '<-5_>=-10%'
		when t1.proc_status_mob_no_vas < -10 and t1.proc_status_mob_no_vas >= -15 then '<-10_>=-15%'
		when t1.proc_status_mob_no_vas < -15 and t1.proc_status_mob_no_vas >= -20 then '<-15_>=-20%'
		when t1.proc_status_mob_no_vas < -20 and t1.proc_status_mob_no_vas >= -25 then '<-20_>=-25%'
		when t1.proc_status_mob_no_vas < -25 and t1.proc_status_mob_no_vas >= -30 then '<-25_>=-30%'
		when t1.proc_status_mob_no_vas < -30 and t1.proc_status_mob_no_vas >= -35 then '<-30_>=-35%'
		when t1.proc_status_mob_no_vas < -35 and t1.proc_status_mob_no_vas >= -40 then '<-35_>=-40%'
		when t1.proc_status_mob_no_vas < -40 and t1.proc_status_mob_no_vas >= -45 then '<-40_>=-45%'
		when t1.proc_status_mob_no_vas < -45 and t1.proc_status_mob_no_vas >= -50 then '<-45_>=-50%'
		when t1.proc_status_mob_no_vas < -50 and t1.proc_status_mob_no_vas >= -55 then '<-50_>=-55%'
		when t1.proc_status_mob_no_vas < -55 and t1.proc_status_mob_no_vas >= -60 then '<-55_>=-60%'
		when t1.proc_status_mob_no_vas < -60 and t1.proc_status_mob_no_vas >= -65 then '<-60_>=-65%'
		when t1.proc_status_mob_no_vas < -65 and t1.proc_status_mob_no_vas >= -70 then '<-65_>=-70%'
		when t1.proc_status_mob_no_vas < -70 and t1.proc_status_mob_no_vas >= -75 then '<-70_>=-75%'
		when t1.proc_status_mob_no_vas < -75 and t1.proc_status_mob_no_vas >= -80 then '<-75_>=-80%'
		when t1.proc_status_mob_no_vas < -80 and t1.proc_status_mob_no_vas >= -85 then '<-80_>=-85%'
		when t1.proc_status_mob_no_vas < -85 and t1.proc_status_mob_no_vas >= -90 then '<-85_>=-90%'
		when t1.proc_status_mob_no_vas < -90 and t1.proc_status_mob_no_vas >= -95 then '<-90_>=-95%'
		when t1.proc_status_mob_no_vas < -95 and t1.proc_status_mob_no_vas >= -100 then '<-95_>=-100%'
		
		when t1.proc_status_mob_no_vas > 0 and t1.proc_status_mob_no_vas <= 5 then '>0_<=5%'
		when t1.proc_status_mob_no_vas > 5 and t1.proc_status_mob_no_vas <= 10 then '>5_<=10%'
		when t1.proc_status_mob_no_vas > 10 and t1.proc_status_mob_no_vas <= 15 then '>10_<=15%'
		when t1.proc_status_mob_no_vas > 15 and t1.proc_status_mob_no_vas <= 20 then '>15_<=20%'
		when t1.proc_status_mob_no_vas > 20 and t1.proc_status_mob_no_vas <= 25 then '>20_<=25%'
		when t1.proc_status_mob_no_vas > 25 and t1.proc_status_mob_no_vas <= 30 then '>25_<=30%'
		when t1.proc_status_mob_no_vas > 30 and t1.proc_status_mob_no_vas <= 35 then '>30_<=35%'
		when t1.proc_status_mob_no_vas > 35 and t1.proc_status_mob_no_vas <= 40 then '>35_<=40%'
		when t1.proc_status_mob_no_vas > 40 and t1.proc_status_mob_no_vas <= 45 then '>40_<=45%'
		when t1.proc_status_mob_no_vas > 45 and t1.proc_status_mob_no_vas <= 50 then '>45_<=50%'
		when t1.proc_status_mob_no_vas > 50 and t1.proc_status_mob_no_vas <= 55 then '>50_<=55%'
		when t1.proc_status_mob_no_vas > 55 and t1.proc_status_mob_no_vas <= 60 then '>55_<=60%'
		when t1.proc_status_mob_no_vas > 60 and t1.proc_status_mob_no_vas <= 65 then '>60_<=65%'
		when t1.proc_status_mob_no_vas > 65 and t1.proc_status_mob_no_vas <= 70 then '>65_<=70%'
		when t1.proc_status_mob_no_vas > 70 and t1.proc_status_mob_no_vas <= 75 then '>70_<=75%'
		when t1.proc_status_mob_no_vas > 75 and t1.proc_status_mob_no_vas <= 80 then '>75_<=80%'
		when t1.proc_status_mob_no_vas > 80 and t1.proc_status_mob_no_vas <= 85 then '>80_<=85%'
		when t1.proc_status_mob_no_vas > 85 and t1.proc_status_mob_no_vas <= 90 then '>85_<=90%'
		when t1.proc_status_mob_no_vas > 90 and t1.proc_status_mob_no_vas <= 95 then '>90_<=95%'
		when t1.proc_status_mob_no_vas > 95 and t1.proc_status_mob_no_vas <= 100 then '>95_<=100%'
		when t1.proc_status_mob_no_vas > 100 then '>100%'
		
		else '0' end as categ_proc_status_mob_no_vas
	
	,t1.sum_status_mob_total
	,t1.sum_status_mob_no_vas
	
	,t1.tariff_id_as_is
	,t1.tariff_as_is
	,t1.tp_group_id_as_is
	,t1.tp_group_as_is
	,t1.tp_big_as_is
	,t1.m_categ_id_as_is
	,t1.m_categ_as_is
	,t1.segment_as_is
	,t1.inn_as_is
    ,t1.kontr_name_as_is
	
	,t1.mark_inn_m2m_iot
	,1 as count_abonent
	
from vrem_25 t1
)with data distributed randomly;
analyze vrem_26;



-- создаем vrem_27

drop table if exists vrem_27;
create temporary table vrem_27 
		select
	a.period
	,a.period_date
	,a.regid
	,a.app_n
	,a.phone_num
	,a.con_n
	,a.inn
	,a.is_con_inn
	,a.kontr_name
	,a.abonent_activation_date
	,a.abonent_activation_month
	,a.activation_marker
	,a.region
	,a.region_txt
	,a.Reg_Week2
	,a.TR
	
	,a.tariff_mgr
	,a.tariff_id_prev
	,a.tariff_id
	,a.tariff_prev
	,a.tariff
	
	,a.tp_group_mgr
	,a.tp_group_id_prev
	,a.tp_group_id
	,a.tp_group_prev
	,a.tp_group
	
	,a.tp_big_mgr
	,a.tp_big_prev
	,a.tp_big
		
	,a.m_categ_mgr
	,a.m_categ_id_prev
	,a.m_categ_id
	,a.m_categ_prev
	,a.m_categ
	
	,a.segment_mgr
	,a.segment_prev
	,a.segment
	,a.smena_segmenta
	
	,a.fclc_prochie_mob
	,a.fclc_prochie_vas
	,a.fclc_golos
	,a.fclc_gprs
	,a.fclc_sms
	,a.fclc_vsr
	,a.fclc_vz_mg_mn
	,a.fclc_abon_plata
	,a.fclc_mob_total
	,a.fclc_mob_no_vas
	,a.fclc_total
	,a.fclc_aff_serv
	
	,a.fclc_mob_total_prev
	,a.fclc_mob_no_vas_prev
	,a.fclc_total_prev
	
	,a.fclc_mob_total_x
	,a.fclc_mob_no_vas_x
	,a.fclc_total_x
	
	,a.activnost_mob_total
	,a.activnost_mob_no_vas
	
	,a.ARPU_abonent_categ_mob_total
    ,a.ARPU_abonent_categ_mob_no_vas
	,a.ARPU_abonent_categ_ottoka_mob_total
    ,a.ARPU_abonent_categ_ottoka_mob_no_vas
	
	,a.mark_zak_block
	,a.month_zak_block
	
	,a.metk_aff_ser
	
	,a.pritok_mob_total
	,a.pritok_mob_no_vas
	,a.pritok_mob_total_all
	,a.pritok_mob_no_vas_all
	,a.ottok_mob_total
	,a.ottok_mob_no_vas
	,a.ottok_mob_total_all
	,a.ottok_mob_no_vas_all
	
	,a.avg_do_ottoka_abonenta_mob_total
	,a.avg_do_ottoka_abonenta_mob_no_vas
	,a.rl_po_ottoku_abonenta_mob_total
	,a.rl_po_ottoku_abonenta_mob_no_vas
	,a.ottok_last_month_mob_total 
	,a.ottok_last_month_mob_no_vas 
	
	,a.proc_status_mob_total
	,a.proc_status_mob_no_vas
	,a.categ_proc_status_mob_total
	,a.categ_proc_status_mob_no_vas
	
	,a.sum_status_mob_total
	,a.sum_status_mob_no_vas
	
	,a.tariff_id_as_is
	,a.tariff_as_is
	,a.tp_group_id_as_is
	,a.tp_group_as_is
	,a.tp_big_as_is
	,a.m_categ_id_as_is
	,a.m_categ_as_is
	,a.segment_as_is
	,a.inn_as_is
    ,a.kontr_name_as_is
	
	,a.mark_inn_m2m_iot
	,a.count_abonent
	
	--1 Приток моб тотал
	,case 
		when a.period < a.abonent_activation_month
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and a.tp_big_as_is<>'M2M/IOT' 
			and coalesce(a.fclc_mob_total,0)>0 
		then 'Активный без активации'


		when coalesce(a.fclc_mob_total,0)>0 
			and a.activation_marker=1 and tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Продажа'

		
		when coalesce(a.fclc_mob_total,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(a.fclc_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0
			and LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0
		then 'Миграция из др. бизнеса'

	
		when coalesce(a.fclc_mob_total,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(a.fclc_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0 
		then 'Реактивация'

		
                when coalesce(a.fclc_mob_total,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(a.fclc_mob_total,0)>= 
				(LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*1.15)
		then 'Развитие'
		
		
		when coalesce(a.fclc_mob_total,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(a.fclc_mob_total,0)> 
				(LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*0.85)
			and COALESCE(a.fclc_mob_total,0)< 
				(LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*1.15)
		then 'Стабильно платят'
	
		
		when coalesce(a.fclc_mob_total,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг')
			AND COALESCE(a.fclc_mob_total,0)<=
				(LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*0.85)
		then 'Оптимизация' 
	
	
	else 'Пусто'
	end as pritok_mob_total_as_is
		
		
--2 Приток моб без вас
	,case 
		when a.period < a.abonent_activation_month
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and a.tp_big_as_is<>'M2M/IOT' 
			and coalesce(a.fclc_mob_no_vas,0)>0 
		then 'Активный без активации'
	
	
		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.activation_marker=1 and tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Продажа'

		
		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(a.fclc_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0
			and LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0
		then 'Миграция из др. бизнеса'

	
		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(a.fclc_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0 
		then 'Реактивация'

		
		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(a.fclc_mob_no_vas,0)>= 
				(LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*1.15)
		then 'Развитие'
		
		
		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(a.fclc_mob_no_vas,0)> 
				(LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*0.85)
			and COALESCE(a.fclc_mob_no_vas,0)< 
				(LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*1.15)
		then 'Стабильно платят'
	
		
		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг')
			AND COALESCE(a.fclc_mob_no_vas,0)<=
				(LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*0.85)
		then 'Оптимизация' 
	
	
	else 'Пусто'
	end as pritok_mob_no_vas_as_is		
		
		
--3 Приток моб тотал Общ		
	,case
		when a.period < a.abonent_activation_month
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and a.tp_big_as_is<>'M2M/IOT' 
			and coalesce(a.fclc_mob_total,0)>0 
		then 'Активный без активации'
	
	
		when coalesce(a.fclc_mob_total,0)>0 
			and a.activation_marker=1 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Продажа'


		when coalesce(a.fclc_mob_total,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(a.fclc_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0
			and LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0
		then 'Миграция из др. бизнеса'


		when coalesce(fclc_mob_total,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(a.fclc_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0 
		then 'Реактивация'


		when coalesce(a.fclc_mob_total,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(a.fclc_mob_total,0)>= 
				(LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*1.15)
		then 'Развитие'


		when coalesce(a.fclc_mob_total,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(a.fclc_mob_total,0)> 
				(LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*0.85)
			and COALESCE(a.fclc_mob_total,0)< 
				(LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*1.15)
		then 'Стабильно платят'


		when coalesce(a.fclc_mob_total,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(a.fclc_mob_total,0)<=
				(LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*0.85)
		then 'Оптимизация' 


	else 'Пусто'
	end as pritok_mob_total_all_as_is
	
	
--4 Приток моб без вас Общ
	,case
		when a.period < a.abonent_activation_month
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and a.tp_big_as_is<>'M2M/IOT' 
			and coalesce(a.fclc_mob_no_vas,0)>0 
		then 'Активный без активации'
	
	
		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.activation_marker=1 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Продажа'


		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(a.fclc_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0
			and LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0
		then 'Миграция из др. бизнеса'


		when coalesce(fclc_mob_no_vas,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(COALESCE(a.fclc_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0 
		then 'Реактивация'


		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(a.fclc_mob_no_vas,0)>= 
				(LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*1.15)
		then 'Развитие'


		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(a.fclc_mob_no_vas,0)> 
				(LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*0.85)
			and COALESCE(a.fclc_mob_no_vas,0)< 
				(LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*1.15)
		then 'Стабильно платят'


		when coalesce(a.fclc_mob_no_vas,0)>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			AND COALESCE(a.fclc_mob_no_vas,0)<=
				(LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)*0.85)
		then 'Оптимизация' 


	else 'Пусто'
	end as pritok_mob_no_vas_all_as_is	
	

--5 Отток моб тотал
	,CASE 
		when LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0 
			and coalesce(a.fclc_mob_total, 0)=0 
		then 'Ранее перешёл в отток'

	
		when LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0 
			and coalesce(a.fclc_mob_total, 0)=0 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(a.period) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<a.abonent_activation_month
		then 'Отток с активацией'
	
	
		when coalesce(a.fclc_mob_total,0)=0 
			and LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0 
			and coalesce(a.fclc_total,0)=0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
			'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Отток'


		when LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0
			and coalesce(a.fclc_mob_total,0)=0            
			and coalesce(a.fclc_total,0)<>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в др. бизнес'


		
	else 'Пусто'
	end as ottok_mob_total_as_is
	
	
--6 Отток моб без вас	
	,CASE 
		when LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0 
			and coalesce(a.fclc_mob_no_vas, 0)=0 
		then 'Ранее перешёл в отток'


		when LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0 
			and coalesce(a.fclc_mob_no_vas, 0)=0 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(a.period) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<a.abonent_activation_month
		then 'Отток с активацией'


		when coalesce(a.fclc_mob_no_vas,0)=0 
			and LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0 
			and coalesce(a.fclc_total,0)=0 
			and a.tp_big_prev<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Отток'


		when LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0 
			and coalesce(a.fclc_mob_no_vas,0)=0
			and coalesce(a.fclc_total,0)<>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в др. бизнес'


		
	else 'Пусто'
	end as ottok_mob_no_vas_as_is	
	
	
--7 Отток моб тотал Общ
	,CASE 
		when LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0 
			and coalesce(a.fclc_mob_total,0)=0 
		then 'Ранее перешёл в отток'

	
		when LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0 
			and coalesce(a.fclc_mob_total, 0)=0 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(a.period) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<a.abonent_activation_month
		then 'Отток с активацией'
	
	
		when coalesce(a.fclc_mob_total,0)=0 
			and LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0 
			and coalesce(a.fclc_total, 0)=0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Отток'


		when LAG(COALESCE(a.fclc_mob_total,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0
			and coalesce(a.fclc_mob_total,0)=0            
			and coalesce(a.fclc_total,0)<>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в др. бизнес'


		
	else 'Пусто' 
	end as ottok_mob_total_all_as_is


--8 Отток моб без вас Общ
	,CASE 
		when LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)=0 
			and coalesce(a.fclc_mob_no_vas,0)=0 
		then 'Ранее перешёл в отток'

	
		when LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0 
			and coalesce(a.fclc_mob_no_vas, 0)=0 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
			and LAG(a.period) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<a.abonent_activation_month
		then 'Отток с активацией'
	
	
		when coalesce(a.fclc_mob_no_vas,0)=0 
			and LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0 
			and coalesce(a.fclc_total, 0)=0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Отток'


		when LAG(COALESCE(a.fclc_mob_no_vas,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period)<>0 
			and coalesce(a.fclc_mob_no_vas,0)=0
			and coalesce(a.fclc_total,0)<>0 
			and a.tp_big_as_is<>'M2M/IOT' 
			and a.segment_as_is IN ('Микро предприятия', 'Малый бизнес', 'Средний бизнес', 'Крупный бизнес', 
				'Национальные клиенты', 'Гос_заказчики', 'Свой круг') 
		then 'Миграция в др. бизнес'

	
	else 'Пусто' 
	end as ottok_mob_no_vas_all_as_is
	
from vrem_26 a
		
)with data distributed randomly;
analyze vrem_27;


-- создаем vr_mob_money_no_vas_ser

drop table if exists vr_mob_money_no_vas_ser;
create temporary table vr_mob_money_no_vas_ser 
		SELECT 
    t1.regid 
    , t1.app_n 
    , t1.service_id1
	, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
	, t2.product_pl_new
    , sum(t1.fclc_trans_part) as sum_fclc_no_vas 
FROM schema_name.detail_analit_mob_santalov as t1
left join schema_name.guide_accounts_sms as t2
		on round(t1.group_id, 0) = round(t2.group_id, 0) 
		and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM vrem_27 a) 
    and lower(t1.service_id1) not like '%штраф%'
    and lower(t1.service_id1) not like '%дебет%коррект%'
    and lower(t1.service_id1) not like '%mobile%сервис%'
    and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
	and lower(t2.product_pl_new) = 'моб'
	and lower(t2.product_pl_det2) <> 'прочие vas'
group by
    t1.regid
    , t1.app_n
    , t1.service_id1
    , period
	, t2.product_pl_new
)with data distributed randomly;
analyze vr_mob_money_no_vas_ser;





-- создаем vr_mob_serv_1

drop table if exists vr_mob_serv_1;
create temporary table vr_mob_serv_1 
		SELECT 
    t1.regid 
    , t1.app_n 
    , t1.service_id1
    , t2.service_type_one
    , t2.service_type_two
	, t1.period 
	, t1.product_pl_new
    , t1.sum_fclc_no_vas 
FROM vr_mob_money_no_vas_ser as t1
left join schema_name.guide_serv_type as t2
		on lower(t1.service_id1) = lower(t2.service_txt)
)with data distributed randomly;
analyze vr_mob_serv_1;


-- создаем table_correct_202301_2_money_no_vas_ser

drop table if exists table_correct_202301_2_money_no_vas_ser;
create temporary table table_correct_202301_2_money_no_vas_ser 
		SELECT 
    t1.regid 
    , t1.app_n 
    , t1.service_id1
	, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
	, t2.product_pl_new
    , sum(t1.fclc_trans_part) as sum_fclc_no_vas 
FROM table_correct_202301_2 as t1
left join schema_name.guide_accounts_sms as t2
		on round(t1.group_id, 0) = round(t2.group_id, 0) 
		and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM analyticsb2b_sb.table_vr_mob_vrem_27 a) 
    and lower(t1.service_id1) not like '%штраф%'
    and lower(t1.service_id1) not like '%дебет%коррект%'
    and lower(t1.service_id1) not like '%mobile%сервис%'
    and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
	and lower(t2.product_pl_new) = 'моб'
	and lower(t2.product_pl_det2) <> 'прочие vas'
group by
    t1.regid
    , t1.app_n
    , t1.service_id1
    , period
	, t2.product_pl_new
)with data distributed randomly;
analyze table_correct_202301_2_money_no_vas_ser;


-- создаем table_correct_202301_2_serv_1

drop table if exists table_correct_202301_2_serv_1;
create temporary table table_correct_202301_2_serv_1 
		SELECT 
    t1.regid 
    , t1.app_n 
    , t1.service_id1
    , t2.service_type_one
    , t2.service_type_two
	, t1.period 
	, t1.product_pl_new
    , t1.sum_fclc_no_vas 
FROM table_correct_202301_2_money_no_vas_ser as t1
left join schema_name.guide_serv_type as t2
		on lower(t1.service_id1) = lower(t2.service_txt)
)with data distributed randomly;
analyze table_correct_202301_2_serv_1;


-- создаем table_correct_202302_2_money_no_vas_ser

drop table if exists table_correct_202302_2_money_no_vas_ser;
create temporary table table_correct_202302_2_money_no_vas_ser 
		SELECT 
    t1.regid 
    , t1.app_n 
    , t1.service_id1
	, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
	, t2.product_pl_new
    , sum(t1.fclc_trans_part) as sum_fclc_no_vas 
FROM table_correct_202302_2 as t1
left join schema_name.guide_accounts_sms as t2
		on round(t1.group_id, 0) = round(t2.group_id, 0) 
		and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM analyticsb2b_sb.table_vr_mob_vrem_27 a) 
    and lower(t1.service_id1) not like '%штраф%'
    and lower(t1.service_id1) not like '%дебет%коррект%'
    and lower(t1.service_id1) not like '%mobile%сервис%'
    and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
	and lower(t2.product_pl_new) = 'моб'
	and lower(t2.product_pl_det2) <> 'прочие vas'
group by
    t1.regid
    , t1.app_n
    , t1.service_id1
    , period
	, t2.product_pl_new
)with data distributed randomly;
analyze table_correct_202302_2_money_no_vas_ser;


-- создаем table_correct_202302_2_serv_1

drop table if exists table_correct_202302_2_serv_1;
create temporary table table_correct_202302_2_serv_1 
		SELECT 
    t1.regid 
    , t1.app_n 
    , t1.service_id1
    , t2.service_type_one
    , t2.service_type_two
	, t1.period 
	, t1.product_pl_new
    , t1.sum_fclc_no_vas 
FROM table_correct_202302_2_money_no_vas_ser as t1
left join schema_name.guide_serv_type as t2
		on lower(t1.service_id1) = lower(t2.service_txt)
)with data distributed randomly;
analyze table_correct_202302_2_serv_1;


-- создаем table_correct_202303_2_money_no_vas_ser

drop table if exists table_correct_202303_2_money_no_vas_ser;
create temporary table table_correct_202303_2_money_no_vas_ser 
		SELECT 
    t1.regid 
    , t1.app_n 
    , t1.service_id1
	, date_format(to_date(t1.table_business_month), 'yyyyMM') as period 
	, t2.product_pl_new
    , sum(t1.fclc_trans_part) as sum_fclc_no_vas 
FROM table_correct_202303_2 as t1
left join schema_name.guide_accounts_sms as t2
		on round(t1.group_id, 0) = round(t2.group_id, 0) 
		and left(t1.sub_acc_trans, 8) = t2.sub_acc_trans
WHERE t1.regid||t1.app_n IN (SELECT distinct a.regid||a.app_n FROM analyticsb2b_sb.table_vr_mob_vrem_27 a) 
    and lower(t1.service_id1) not like '%штраф%'
    and lower(t1.service_id1) not like '%дебет%коррект%'
    and lower(t1.service_id1) not like '%mobile%сервис%'
    and lower(t1.service_id1) not like 'b2b%оборудование%обслуживание%новая%цена%'
	and lower(t2.product_pl_new) = 'моб'
	and lower(t2.product_pl_det2) <> 'прочие vas'
group by
    t1.regid
    , t1.app_n
    , t1.service_id1
    , period
	, t2.product_pl_new
)with data distributed randomly;
analyze table_correct_202303_2_money_no_vas_ser;


-- создаем table_correct_202303_2_serv_1

drop table if exists table_correct_202303_2_serv_1;
create temporary table table_correct_202303_2_serv_1 
		SELECT 
    t1.regid 
    , t1.app_n 
    , t1.service_id1
    , t2.service_type_one
    , t2.service_type_two
	, t1.period 
	, t1.product_pl_new
    , t1.sum_fclc_no_vas 
FROM table_correct_202303_2_money_no_vas_ser as t1
left join schema_name.guide_serv_type as t2
		on lower(t1.service_id1) = lower(t2.service_txt)
)with data distributed randomly;
analyze table_correct_202303_2_serv_1;

-- создаем vr_mob_serv_all

drop table if exists vr_mob_serv_all;
create temporary table vr_mob_serv_all 

	SELECT 
    t1.regid 
    , t1.app_n 
    , t1.service_id1
    , t1.service_type_one
    , t1.service_type_two
	, t1.period
    , t1.sum_fclc_no_vas 
FROM vr_mob_serv_1 t1
where t1.period not in ('202301', '202302', '202303')
union ALL
SELECT 
    t2.regid 
    , t2.app_n 
    , t2.service_id1
    , t2.service_type_one
    , t2.service_type_two
	, t2.period
    , t2.sum_fclc_no_vas 
FROM table_correct_202301_2_serv_1 t2
union ALL
SELECT 
    t3.regid 
    , t3.app_n 
    , t3.service_id1
    , t3.service_type_one
    , t3.service_type_two
	, t3.period
    , t3.sum_fclc_no_vas 
FROM table_correct_202302_2_serv_1 t3
union ALL
SELECT 
    t4.regid 
    , t4.app_n 
    , t4.service_id1
    , t4.service_type_one
    , t4.service_type_two
	, t4.period
    , t4.sum_fclc_no_vas 
FROM table_correct_202303_2_serv_1 t4
		
)with data distributed randomly;
analyze vr_mob_serv_all;



-- создаем vr_mob_serv_all_1

drop table if exists vr_mob_serv_all_1;
create temporary table vr_mob_serv_all_1 
	SELECT 
    t1.period
	, t1.regid 
    , t1.app_n 
    , t1.service_id1
    , t1.service_type_one
    , t1.service_type_two
	, case 
		when lower(t1.service_id1) like '%держав%' then t1.sum_fclc_no_vas
		else 0 end as fclc_derzava1
	, case 
		when lower(t1.service_id1) like '%плата%при%подключ%' then t1.sum_fclc_no_vas
		else 0 end as fclc_connect1
	, case 
		when lower(t1.service_id1) not like '%держав%' 
			and lower(t1.service_id1) not like '%плата%при%подключ%'
			and lower(t1.service_type_two) = 'периодическая' then t1.sum_fclc_no_vas
		else 0 end as fclc_periodika1
	, case 
		when lower(t1.service_id1) not like '%держав%' 
			and lower(t1.service_id1) not like '%плата%при%подключ%'
			and lower(t1.service_type_two) = 'трафик' then t1.sum_fclc_no_vas
		else 0 end as fclc_trafik1
	, case 
		when lower(t1.service_id1) not like '%держав%' 
			and lower(t1.service_id1) not like '%плата%при%подключ%'
			and lower(t1.service_type_two) = 'ба' then t1.sum_fclc_no_vas
		else 0 end as fclc_bus_abon1
	, case 
		when lower(t1.service_id1) not like '%держав%' 
			and lower(t1.service_id1) not like '%плата%при%подключ%'
			and lower(t1.service_type_two) = 'кат номер' then t1.sum_fclc_no_vas
		else 0 end as fclc_cat_number1
	, case 
		when lower(t1.service_id1) not like '%держав%' 
			and lower(t1.service_id1) not like '%плата%при%подключ%'
			and lower(t1.service_type_two) = 'инстал' then t1.sum_fclc_no_vas
		else 0 end as fclc_instal1
	, case 
		when lower(t1.service_id1) not like '%держав%' 
			and lower(t1.service_id1) not like '%плата%при%подключ%'
			and lower(t1.service_type_two) = 'разовая' then t1.sum_fclc_no_vas
		else 0 end as fclc_other_one_time1
	, t1.sum_fclc_no_vas 
FROM vr_mob_serv_all t1
	
)with data distributed randomly;
analyze vr_mob_serv_all_1;


-- создаем vr_mob_serv_all_2

drop table if exists vr_mob_serv_all_2;
create temporary table vr_mob_serv_all_2 
	SELECT 
    t1.period
	, t1.regid 
    , t1.app_n
	, sum(t1.fclc_derzava1) as fclc_derzava1
	, sum(t1.fclc_connect1) as fclc_connect1
	, sum(t1.fclc_periodika1) as fclc_periodika1
	, sum(t1.fclc_trafik1) as fclc_trafik1
	, sum(t1.fclc_bus_abon1) as fclc_bus_abon1
	, sum(t1.fclc_cat_number1) as fclc_cat_number1
	, sum(t1.fclc_instal1) as fclc_instal1
	, sum(t1.fclc_other_one_time1) as fclc_other_one_time1
	, sum(t1.sum_fclc_no_vas) as sum_fclc_no_vas
FROM vr_mob_serv_all_1 t1
group BY
	t1.period
	, t1.regid 
    , t1.app_n
)with data distributed randomly;
analyze vr_mob_serv_all_2;



-- создаем vrem_28

drop table if exists vrem_28;
create temporary table vrem_28 
	select
	t1.period
	,t1.period_date
	,t1.regid
	,t1.app_n
	,t1.phone_num
	,t1.con_n
	,t1.inn
	,t1.is_con_inn
	,t1.kontr_name
	,t1.abonent_activation_date
	,t1.abonent_activation_month
	,t1.activation_marker
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	
	,t1.tariff_mgr
	,t1.tariff_id_prev
	,t1.tariff_id
	,t1.tariff_prev
	,t1.tariff
	
	,t1.tp_group_mgr
	,t1.tp_group_id_prev
	,t1.tp_group_id
	,t1.tp_group_prev
	,t1.tp_group
	
	,t1.tp_big_mgr
	,t1.tp_big_prev
	,t1.tp_big
		
	,t1.m_categ_mgr
	,t1.m_categ_id_prev
	,t1.m_categ_id
	,t1.m_categ_prev
	,t1.m_categ
	
	,t1.segment_mgr
	,t1.segment_prev
	,t1.segment
	,t1.smena_segmenta
	
	,t1.fclc_prochie_mob
	,t1.fclc_prochie_vas
	,t1.fclc_golos
	,t1.fclc_gprs
	,t1.fclc_sms
	,t1.fclc_vsr
	,t1.fclc_vz_mg_mn
	,t1.fclc_abon_plata
	,t1.fclc_mob_total
	,t1.fclc_mob_no_vas
	,t1.fclc_total
	,t1.fclc_aff_serv
	
	,t1.fclc_mob_total_prev
	,t1.fclc_mob_no_vas_prev
	,t1.fclc_total_prev
	
	,t1.fclc_mob_total_x
	,t1.fclc_mob_no_vas_x
	,t1.fclc_total_x
	
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	
	,t1.ARPU_abonent_categ_mob_total
    ,t1.ARPU_abonent_categ_mob_no_vas
	,t1.ARPU_abonent_categ_ottoka_mob_total
    ,t1.ARPU_abonent_categ_ottoka_mob_no_vas
	
	,t1.mark_zak_block
	,t1.month_zak_block
	
	,t1.metk_aff_ser
	
	,t1.pritok_mob_total
	,t1.pritok_mob_no_vas
	,t1.pritok_mob_total_all
	,t1.pritok_mob_no_vas_all
	,t1.ottok_mob_total
	,t1.ottok_mob_no_vas
	,t1.ottok_mob_total_all
	,t1.ottok_mob_no_vas_all
	
	,t1.avg_do_ottoka_abonenta_mob_total
	,t1.avg_do_ottoka_abonenta_mob_no_vas
	,t1.rl_po_ottoku_abonenta_mob_total
	,t1.rl_po_ottoku_abonenta_mob_no_vas
	,t1.ottok_last_month_mob_total 
	,t1.ottok_last_month_mob_no_vas 
	
	,t1.proc_status_mob_total
	,t1.proc_status_mob_no_vas
	,t1.categ_proc_status_mob_total
	,t1.categ_proc_status_mob_no_vas
	
	,t1.sum_status_mob_total
	,t1.sum_status_mob_no_vas
	
	,t1.tariff_id_as_is
	,t1.tariff_as_is
	,t1.tp_group_id_as_is
	,t1.tp_group_as_is
	,t1.tp_big_as_is
	,t1.m_categ_id_as_is
	,t1.m_categ_as_is
	,t1.segment_as_is
	,t1.inn_as_is
    ,t1.kontr_name_as_is
	
	,t1.mark_inn_m2m_iot
	,t1.count_abonent
	
	,t1.pritok_mob_total_as_is
	,t1.pritok_mob_no_vas_as_is		
	,t1.pritok_mob_total_all_as_is
	,t1.pritok_mob_no_vas_all_as_is	
	,t1.ottok_mob_total_as_is
	,t1.ottok_mob_no_vas_as_is	
	,t1.ottok_mob_total_all_as_is
	,t1.ottok_mob_no_vas_all_as_is
	
	,case 
		when t1.segment in ('B2C', 'Не в B2C') then 0
		else t2.fclc_derzava1 end as fclc_derzava
	,case 
		when t1.segment in ('B2C', 'Не в B2C') then 0
		else t2.fclc_connect1 end as fclc_connect
	,case 
		when t1.segment in ('B2C', 'Не в B2C') then 0
		else t2.fclc_periodika1 end as fclc_periodika
	,case 
		when t1.segment in ('B2C', 'Не в B2C') then 0
		else t2.fclc_trafik1 end as fclc_trafik
	,case 
		when t1.segment in ('B2C', 'Не в B2C') then 0
		else t2.fclc_bus_abon1 end as fclc_bus_abon
	,case 
		when t1.segment in ('B2C', 'Не в B2C') then 0
		else t2.fclc_cat_number1 end as fclc_cat_number
	,case 
		when t1.segment in ('B2C', 'Не в B2C') then 0
		else t2.fclc_instal1 end as fclc_instal
	,case 
		when t1.segment in ('B2C', 'Не в B2C') then 0
		else t2.fclc_other_one_time1 end as fclc_other_one_time
	
from vrem_27 t1
left join vr_mob_serv_all_2 t2
	on t1.period = t2.period
	and t1.regid = t2.regid
	and t1.app_n = t2.app_n
)with data distributed randomly;
analyze vrem_28;


-- создаем vrem_29

drop table if exists vrem_29;
create temporary table vrem_29 
	select
	t1.period
	,t1.period_date
	,t1.regid
	,t1.app_n
	,t1.phone_num
	,t1.con_n
	,t1.inn
	,t1.is_con_inn
	,t1.kontr_name
	,t1.abonent_activation_date
	,t1.abonent_activation_month
	,t1.activation_marker
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	
	,t1.tariff_mgr
	,t1.tariff_id_prev
	,t1.tariff_id
	,t1.tariff_prev
	,t1.tariff
	
	,t1.tp_group_mgr
	,t1.tp_group_id_prev
	,t1.tp_group_id
	,t1.tp_group_prev
	,t1.tp_group
	
	,t1.tp_big_mgr
	,t1.tp_big_prev
	,t1.tp_big
		
	,t1.m_categ_mgr
	,t1.m_categ_id_prev
	,t1.m_categ_id
	,t1.m_categ_prev
	,t1.m_categ
	
	,t1.segment_mgr
	,t1.segment_prev
	,t1.segment
	,t1.smena_segmenta
	
	,t1.fclc_prochie_mob
	,t1.fclc_prochie_vas
	,t1.fclc_golos
	,t1.fclc_gprs
	,t1.fclc_sms
	,t1.fclc_vsr
	,t1.fclc_vz_mg_mn
	,t1.fclc_abon_plata
	,t1.fclc_mob_total
	,t1.fclc_mob_no_vas
	,t1.fclc_total
	,t1.fclc_aff_serv
	
	,t1.fclc_mob_total_prev
	,t1.fclc_mob_no_vas_prev
	,t1.fclc_total_prev
	
	,t1.fclc_mob_total_x
	,t1.fclc_mob_no_vas_x
	,t1.fclc_total_x
	
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	
	,t1.ARPU_abonent_categ_mob_total
    ,t1.ARPU_abonent_categ_mob_no_vas
	,t1.ARPU_abonent_categ_ottoka_mob_total
    ,t1.ARPU_abonent_categ_ottoka_mob_no_vas
	
	,t1.mark_zak_block
	,t1.month_zak_block
	
	,t1.metk_aff_ser
	
	,t1.pritok_mob_total
	,t1.pritok_mob_no_vas
	,t1.pritok_mob_total_all
	,t1.pritok_mob_no_vas_all
	,t1.ottok_mob_total
	,t1.ottok_mob_no_vas
	,t1.ottok_mob_total_all
	,t1.ottok_mob_no_vas_all
	
	,t1.avg_do_ottoka_abonenta_mob_total
	,t1.avg_do_ottoka_abonenta_mob_no_vas
	,t1.rl_po_ottoku_abonenta_mob_total
	,t1.rl_po_ottoku_abonenta_mob_no_vas
	,t1.ottok_last_month_mob_total 
	,t1.ottok_last_month_mob_no_vas 
	
	,t1.proc_status_mob_total
	,t1.proc_status_mob_no_vas
	,t1.categ_proc_status_mob_total
	,t1.categ_proc_status_mob_no_vas
	
	,t1.sum_status_mob_total
	,t1.sum_status_mob_no_vas
	
	,t1.tariff_id_as_is
	,t1.tariff_as_is
	,t1.tp_group_id_as_is
	,t1.tp_group_as_is
	,t1.tp_big_as_is
	,t1.m_categ_id_as_is
	,t1.m_categ_as_is
	,t1.segment_as_is
	,t1.inn_as_is
    ,t1.kontr_name_as_is
	
	,t1.mark_inn_m2m_iot
	,t1.count_abonent
	
	,t1.pritok_mob_total_as_is
	,t1.pritok_mob_no_vas_as_is		
	,t1.pritok_mob_total_all_as_is
	,t1.pritok_mob_no_vas_all_as_is	
	,t1.ottok_mob_total_as_is
	,t1.ottok_mob_no_vas_as_is	
	,t1.ottok_mob_total_all_as_is
	,t1.ottok_mob_no_vas_all_as_is
	
	,t1.fclc_derzava
	,t1.fclc_connect
	,t1.fclc_periodika
	,t1.fclc_trafik
	,t1.fclc_bus_abon
	,t1.fclc_cat_number
	,t1.fclc_instal
	,t1.fclc_other_one_time
	
	,LAG(COALESCE(t1.fclc_derzava,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_derzava_prev
	,LAG(COALESCE(t1.fclc_connect,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_connect_prev
	,LAG(COALESCE(t1.fclc_periodika,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_periodika_prev
	,LAG(COALESCE(t1.fclc_trafik,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_trafik_prev
	,LAG(COALESCE(t1.fclc_bus_abon,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_bus_abon_prev
	,LAG(COALESCE(t1.fclc_cat_number,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_cat_number_prev
	,LAG(COALESCE(t1.fclc_instal,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_instal_prev
	,LAG(COALESCE(t1.fclc_other_one_time,0)) OVER (PARTITION BY t1.regid, t1.app_n ORDER BY t1.period) as fclc_other_one_time_prev
	
from vrem_28 t1
	
		
)with data distributed randomly;
analyze vrem_29;



-- создаем vrem_30

drop table if exists vrem_30;
create temporary table vrem_30 
	select
	t1.period
	,t1.period_date
	,t1.regid
	,t1.app_n
	,t1.phone_num
	,t1.con_n
	,t1.inn
	,t1.is_con_inn
	,t1.kontr_name
	,t1.abonent_activation_date
	,t1.abonent_activation_month
	,t1.activation_marker
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	
	,t1.tariff_mgr
	,t1.tariff_id_prev
	,t1.tariff_id
	,t1.tariff_prev
	,t1.tariff
	
	,t1.tp_group_mgr
	,t1.tp_group_id_prev
	,t1.tp_group_id
	,t1.tp_group_prev
	,t1.tp_group
	
	,t1.tp_big_mgr
	,t1.tp_big_prev
	,t1.tp_big
		
	,t1.m_categ_mgr
	,t1.m_categ_id_prev
	,t1.m_categ_id
	,t1.m_categ_prev
	,t1.m_categ
	
	,t1.segment_mgr
	,t1.segment_prev
	,t1.segment
	,t1.smena_segmenta
	
	,t1.fclc_prochie_mob
	,t1.fclc_prochie_vas
	,t1.fclc_golos
	,t1.fclc_gprs
	,t1.fclc_sms
	,t1.fclc_vsr
	,t1.fclc_vz_mg_mn
	,t1.fclc_abon_plata
	,t1.fclc_mob_total
	,t1.fclc_mob_no_vas
	,t1.fclc_total
	,t1.fclc_aff_serv
	
	,t1.fclc_mob_total_prev
	,t1.fclc_mob_no_vas_prev
	,t1.fclc_total_prev
	
	,t1.fclc_mob_total_x
	,t1.fclc_mob_no_vas_x
	,t1.fclc_total_x
	
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	
	,t1.ARPU_abonent_categ_mob_total
    ,t1.ARPU_abonent_categ_mob_no_vas
	,t1.ARPU_abonent_categ_ottoka_mob_total
    ,t1.ARPU_abonent_categ_ottoka_mob_no_vas
	
	,t1.mark_zak_block
	,t1.month_zak_block
	
	,t1.metk_aff_ser
	
	,t1.pritok_mob_total
	,t1.pritok_mob_no_vas
	,t1.pritok_mob_total_all
	,t1.pritok_mob_no_vas_all
	,t1.ottok_mob_total
	,t1.ottok_mob_no_vas
	,t1.ottok_mob_total_all
	,t1.ottok_mob_no_vas_all
	
	,t1.avg_do_ottoka_abonenta_mob_total
	,t1.avg_do_ottoka_abonenta_mob_no_vas
	,t1.rl_po_ottoku_abonenta_mob_total
	,t1.rl_po_ottoku_abonenta_mob_no_vas
	,t1.ottok_last_month_mob_total 
	,t1.ottok_last_month_mob_no_vas 
	
	,t1.proc_status_mob_total
	,t1.proc_status_mob_no_vas
	,t1.categ_proc_status_mob_total
	,t1.categ_proc_status_mob_no_vas
	
	,t1.sum_status_mob_total
	,t1.sum_status_mob_no_vas
	
	,t1.tariff_id_as_is
	,t1.tariff_as_is
	,t1.tp_group_id_as_is
	,t1.tp_group_as_is
	,t1.tp_big_as_is
	,t1.m_categ_id_as_is
	,t1.m_categ_as_is
	,t1.segment_as_is
	,t1.inn_as_is
    ,t1.kontr_name_as_is
	
	,t1.mark_inn_m2m_iot
	,t1.count_abonent
	
	,t1.pritok_mob_total_as_is
	,t1.pritok_mob_no_vas_as_is		
	,t1.pritok_mob_total_all_as_is
	,t1.pritok_mob_no_vas_all_as_is	
	,t1.ottok_mob_total_as_is
	,t1.ottok_mob_no_vas_as_is	
	,t1.ottok_mob_total_all_as_is
	,t1.ottok_mob_no_vas_all_as_is
	
	,t1.fclc_derzava
	,t1.fclc_connect
	,t1.fclc_periodika
	,t1.fclc_trafik
	,t1.fclc_bus_abon
	,t1.fclc_cat_number
	,t1.fclc_instal
	,t1.fclc_other_one_time
	
	,t1.fclc_derzava_prev
	,t1.fclc_connect_prev
	,t1.fclc_periodika_prev
	,t1.fclc_trafik_prev
	,t1.fclc_bus_abon_prev
	,t1.fclc_cat_number_prev
	,t1.fclc_instal_prev
	,t1.fclc_other_one_time_prev
	
	,CASE
		when COALESCE(t1.fclc_mob_total,0) > 0 then 1
		else 0 end as flag_clc_mob_total
	,CASE
		when COALESCE(t1.fclc_mob_no_vas,0) > 0 then 1
		else 0 end as flag_clc_mob_no_vas 
	,CASE
		when COALESCE(t1.fclc_mob_total_prev,0) > 0 then 1
		else 0 end as flag_clc_mob_total_prev
	,CASE
		when COALESCE(t1.fclc_mob_no_vas_prev,0) > 0 then 1
		else 0 end as flag_clc_mob_no_vas_prev
	,CASE
		when COALESCE(t1.fclc_periodika,0) > 0 then 1
		else 0 end as flag_clc_fclc_periodika
	,CASE
		when COALESCE(t1.fclc_periodika_prev,0) > 0 then 1
		else 0 end as flag_clc_fclc_periodika_prev
	,CASE
		when COALESCE(t1.fclc_bus_abon,0) > 0 then 1
		else 0 end as flag_clc_fclc_bus_abon
	,CASE
		when COALESCE(t1.fclc_bus_abon_prev,0) > 0 then 1
		else 0 end as flag_clc_fclc_bus_abon_prev
	,CASE
		when COALESCE(t1.fclc_cat_number,0) > 0 then 1
		else 0 end as flag_clc_fclc_cat_number
	,CASE
		when COALESCE(t1.fclc_cat_number_prev,0) > 0 then 1
		else 0 end as flag_clc_fclc_cat_number_prev
		
from vrem_29 t1
	
)with data distributed randomly;
analyze vrem_30;


-- создаем mob_analit_end

drop table if exists mob_analit_end;
create temporary table mob_analit_end 
SELECT
a.*
,LAG(COALESCE(a.fclc_MNR,0)) OVER (PARTITION BY a.regid, a.app_n ORDER BY a.period) as fclc_MNR_prev
from
(
select
	t1.period
	,t1.period_date
	,t1.regid
	,t1.app_n
	,t1.phone_num
	,t1.con_n
	,t1.inn
	,t1.is_con_inn
	,t1.kontr_name
	,t1.abonent_activation_date
	,t1.abonent_activation_month
	,t1.activation_marker
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	
	,t1.tariff_mgr
	,t1.tariff_id_prev
	,t1.tariff_id
	,t1.tariff_prev
	,t1.tariff
	
	,t1.tp_group_mgr
	,t1.tp_group_id_prev
	,t1.tp_group_id
	,t1.tp_group_prev
	,t1.tp_group
	
	,t1.tp_big_mgr
	,t1.tp_big_prev
	,t1.tp_big
		
	,t1.m_categ_mgr
	,t1.m_categ_id_prev
	,t1.m_categ_id
	,t1.m_categ_prev
	,t1.m_categ
	
	,t1.segment_mgr
	,t1.segment_prev
	,t1.segment
	,t1.smena_segmenta
	
	,t1.fclc_prochie_mob
	,t1.fclc_prochie_vas
	,t1.fclc_golos
	,t1.fclc_gprs
	,t1.fclc_sms
	,t1.fclc_vsr
	,t1.fclc_vz_mg_mn
	,t1.fclc_abon_plata
	,t1.fclc_mob_total
	,t1.fclc_mob_no_vas
	,t1.fclc_total
	,t1.fclc_aff_serv
	
	,t1.fclc_mob_total_prev
	,t1.fclc_mob_no_vas_prev
	,t1.fclc_total_prev
	
	,t1.fclc_mob_total_x
	,t1.fclc_mob_no_vas_x
	,t1.fclc_total_x
	
	,t1.activnost_mob_total
	,t1.activnost_mob_no_vas
	
	,t1.ARPU_abonent_categ_mob_total
    ,t1.ARPU_abonent_categ_mob_no_vas
	,t1.ARPU_abonent_categ_ottoka_mob_total
    ,t1.ARPU_abonent_categ_ottoka_mob_no_vas
	
	,t1.mark_zak_block
	,t1.month_zak_block
	
	,t1.metk_aff_ser
	
	,t1.pritok_mob_total
	,t1.pritok_mob_no_vas
	,t1.pritok_mob_total_all
	,t1.pritok_mob_no_vas_all
	,t1.ottok_mob_total
	,t1.ottok_mob_no_vas
	,t1.ottok_mob_total_all
	,t1.ottok_mob_no_vas_all
	
	,t1.avg_do_ottoka_abonenta_mob_total
	,t1.avg_do_ottoka_abonenta_mob_no_vas
	,t1.rl_po_ottoku_abonenta_mob_total
	,t1.rl_po_ottoku_abonenta_mob_no_vas
	,t1.ottok_last_month_mob_total 
	,t1.ottok_last_month_mob_no_vas 
	
	,t1.proc_status_mob_total
	,t1.proc_status_mob_no_vas
	,t1.categ_proc_status_mob_total
	,t1.categ_proc_status_mob_no_vas
	
	,t1.sum_status_mob_total
	,t1.sum_status_mob_no_vas
	
	,t1.tariff_id_as_is
	,t1.tariff_as_is
	,t1.tp_group_id_as_is
	,t1.tp_group_as_is
	,t1.tp_big_as_is
	,t1.m_categ_id_as_is
	,t1.m_categ_as_is
	,t1.segment_as_is
	,t1.inn_as_is
    ,t1.kontr_name_as_is
	
	,t1.mark_inn_m2m_iot
	,t1.count_abonent
	
	,t1.pritok_mob_total_as_is
	,t1.pritok_mob_no_vas_as_is		
	,t1.pritok_mob_total_all_as_is
	,t1.pritok_mob_no_vas_all_as_is	
	,t1.ottok_mob_total_as_is
	,t1.ottok_mob_no_vas_as_is	
	,t1.ottok_mob_total_all_as_is
	,t1.ottok_mob_no_vas_all_as_is
	
	,t1.fclc_derzava
	,t1.fclc_connect
	,t1.fclc_periodika
	,t1.fclc_trafik
	,t1.fclc_bus_abon
	,t1.fclc_cat_number
	,t1.fclc_instal
	,t1.fclc_other_one_time
	
	,t1.fclc_derzava_prev
	,t1.fclc_connect_prev
	,t1.fclc_periodika_prev
	,t1.fclc_trafik_prev
	,t1.fclc_bus_abon_prev
	,t1.fclc_cat_number_prev
	,t1.fclc_instal_prev
	,t1.fclc_other_one_time_prev
	
	,t1.flag_clc_mob_total
	,t1.flag_clc_mob_no_vas 
	,t1.flag_clc_mob_total_prev
	,t1.flag_clc_mob_no_vas_prev
	,t1.flag_clc_fclc_periodika
	,t1.flag_clc_fclc_periodika_prev
	,t1.flag_clc_fclc_bus_abon
	,t1.flag_clc_fclc_bus_abon_prev
	,t1.flag_clc_fclc_cat_number
	,t1.flag_clc_fclc_cat_number_prev
	
	,t2.fclc as fclc_MNR
		
from vrem_30 t1
left join analyticsb2b_sb.table_mnr_agr t2 -- создать видимо
	on t1.period = t2.period
	and t1.regid = t2.regid
	and t1.app_n = t2.app_n
) a

	
		
)with data distributed randomly;
analyze mob_analit_end;


-- создаем предитог inserts_table

drop table if exists inserts_table;
create temporary table inserts_table 

	select
	t1.period
	,t1.inn
	,t1.inn_as_is
	,t1.is_con_inn
	,t1.kontr_name
	,t1.kontr_name_as_is
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	,t1.tariff_id_prev
	,t1.tariff_prev
	,t1.tariff_id
	,t1.tariff
	,t1.tariff_mgr
	,t1.tariff_id_as_is
	,t1.tariff_as_is
	,t1.tp_group_id_prev
	,t1.tp_group_prev
	,t1.tp_group_id
	,t1.tp_group
	,t1.tp_group_mgr
	,t1.tp_group_id_as_is
	,t1.tp_group_as_is
	,t1.tp_big_prev
	,t1.tp_big
	,t1.tp_big_mgr
	,t1.tp_big_as_is
	,t1.m_categ_id_prev
	,t1.m_categ_prev
	,t1.m_categ_id
	,t1.m_categ
	,t1.m_categ_mgr
	,t1.m_categ_id_as_is
	,t1.m_categ_as_is
	,t1.segment_prev
	,t1.segment
	,t1.segment_mgr
	,t1.segment_as_is
	,t1.activation_marker
	,t1.abonent_activation_month
	,t1.ARPU_abonent_categ_mob_total
	,t1.ARPU_abonent_categ_mob_no_vas
	
	,sum(t1.fclc_total) as fclc_total
	,sum(t1.fclc_prochie_mob) as fclc_prochie_mob
	,sum(t1.fclc_gprs) as fclc_gprs
	,sum(t1.fclc_sms) as fclc_sms
	,sum(t1.fclc_vsr) as fclc_vsr
	,sum(t1.fclc_vz_mg_mn) as fclc_vz_mg_mn
	,sum(t1.fclc_abon_plata) as fclc_abon_plata
	,sum(t1.fclc_prochie_vas) as fclc_prochie_vas
	,sum(t1.fclc_mob_total) as fclc_mob_total
	,sum(t1.fclc_mob_no_vas) as fclc_mob_no_vas
	,sum(t1.fclc_total_prev) as fclc_total_prev
	,sum(t1.fclc_mob_total_prev) as fclc_mob_total_prev
	,sum(t1.fclc_mob_no_vas_prev) as fclc_mob_no_vas_prev
	
	,t1.pritok_mob_total
	,t1.pritok_mob_no_vas
	,t1.pritok_mob_total_all
	,t1.pritok_mob_no_vas_all
	,t1.ottok_mob_total
	,t1.ottok_mob_no_vas
	,t1.ottok_mob_total_all
	,t1.ottok_mob_no_vas_all
	,t1.pritok_mob_total_as_is
	,t1.pritok_mob_no_vas_as_is
	,t1.pritok_mob_total_all_as_is
	,t1.pritok_mob_no_vas_all_as_is
	,t1.ottok_mob_total_as_is
	,t1.ottok_mob_no_vas_as_is
	,t1.ottok_mob_total_all_as_is
	,t1.ottok_mob_no_vas_all_as_is
	,t1.ARPU_abonent_categ_ottoka_mob_total
	,t1.ARPU_abonent_categ_ottoka_mob_no_vas
	
	,sum(t1.avg_do_ottoka_abonenta_mob_total) as avg_do_ottoka_abonenta_mob_total
	,sum(t1.avg_do_ottoka_abonenta_mob_no_vas) as avg_do_ottoka_abonenta_mob_no_vas
	,sum(t1.rl_po_ottoku_abonenta_mob_total) as rl_po_ottoku_abonenta_mob_total
	,sum(t1.rl_po_ottoku_abonenta_mob_no_vas) as rl_po_ottoku_abonenta_mob_no_vas
	,sum(t1.sum_status_mob_total) as sum_status_mob_total
	,sum(t1.sum_status_mob_no_vas) as sum_status_mob_no_vas
	
	,t1.categ_proc_status_mob_total
	,t1.categ_proc_status_mob_no_vas
	,t1.ottok_last_month_mob_total
	,t1.ottok_last_month_mob_no_vas
	,t1.mark_zak_block
	,t1.month_zak_block
	,t1.metk_aff_ser as mark_aff_service
	,sum(t1.fclc_aff_serv) as fclc_aff_serv
	,sum(t1.count_abonent) as count_abonent
	
	,sum(t1.fclc_derzava) as fclc_derzava
	,sum(t1.fclc_periodika) as fclc_periodika
	,sum(t1.fclc_trafik) as fclc_trafik
	,sum(t1.fclc_bus_abon) as fclc_bus_abon
	,sum(t1.fclc_cat_number) as fclc_cat_number
	,sum(t1.fclc_connect) as fclc_connect
	,sum(t1.fclc_instal) as fclc_instal
	,sum(t1.fclc_other_one_time) as fclc_other_one_time
	
	,sum(t1.fclc_derzava_prev) as fclc_derzava_prev
	,sum(t1.fclc_periodika_prev) as fclc_periodika_prev
	,sum(t1.fclc_trafik_prev) as fclc_trafik_prev
	,sum(t1.fclc_bus_abon_prev) as fclc_bus_abon_prev
	,sum(t1.fclc_cat_number_prev) as fclc_cat_number_prev
	,sum(t1.fclc_connect_prev) as fclc_connect_prev
	,sum(t1.fclc_instal_prev) as fclc_instal_prev
	,sum(t1.fclc_other_one_time_prev) as fclc_other_one_time_prev
	
	,t1.flag_clc_mob_total
	,t1.flag_clc_mob_no_vas 
	,t1.flag_clc_mob_total_prev
	,t1.flag_clc_mob_no_vas_prev
	,t1.flag_clc_fclc_periodika
	,t1.flag_clc_fclc_periodika_prev
	,t1.flag_clc_fclc_bus_abon
	,t1.flag_clc_fclc_bus_abon_prev
	,t1.flag_clc_fclc_cat_number
	,t1.flag_clc_fclc_cat_number_prev
	
	,sum(t1.fclc_MNR) as fclc_MNR
	,sum(t1.fclc_MNR_prev) as fclc_MNR_prev
	
from mob_analit_end t1
group by 
	t1.period
	,t1.inn
	,t1.inn_as_is
	,t1.is_con_inn
	,t1.kontr_name
	,t1.kontr_name_as_is
	,t1.region
	,t1.region_txt
	,t1.Reg_Week2
	,t1.TR
	,t1.tariff_id_prev
	,t1.tariff_prev
	,t1.tariff_id
	,t1.tariff
	,t1.tariff_mgr
	,t1.tariff_id_as_is
	,t1.tariff_as_is
	,t1.tp_group_id_prev
	,t1.tp_group_prev
	,t1.tp_group_id
	,t1.tp_group
	,t1.tp_group_mgr
	,t1.tp_group_id_as_is
	,t1.tp_group_as_is
	,t1.tp_big_prev
	,t1.tp_big
	,t1.tp_big_mgr
	,t1.tp_big_as_is
	,t1.m_categ_id_prev
	,t1.m_categ_prev
	,t1.m_categ_id
	,t1.m_categ
	,t1.m_categ_mgr
	,t1.m_categ_id_as_is
	,t1.m_categ_as_is
	,t1.segment_prev
	,t1.segment
	,t1.segment_mgr
	,t1.segment_as_is
	,t1.activation_marker
	,t1.abonent_activation_month
	,t1.ARPU_abonent_categ_mob_total
	,t1.ARPU_abonent_categ_mob_no_vas
	,t1.pritok_mob_total
	,t1.pritok_mob_no_vas
	,t1.pritok_mob_total_all
	,t1.pritok_mob_no_vas_all
	,t1.ottok_mob_total
	,t1.ottok_mob_no_vas
	,t1.ottok_mob_total_all
	,t1.ottok_mob_no_vas_all
	,t1.pritok_mob_total_as_is
	,t1.pritok_mob_no_vas_as_is
	,t1.pritok_mob_total_all_as_is
	,t1.pritok_mob_no_vas_all_as_is
	,t1.ottok_mob_total_as_is
	,t1.ottok_mob_no_vas_as_is
	,t1.ottok_mob_total_all_as_is
	,t1.ottok_mob_no_vas_all_as_is
	,t1.ARPU_abonent_categ_ottoka_mob_total
	,t1.ARPU_abonent_categ_ottoka_mob_no_vas
	,t1.categ_proc_status_mob_total
	,t1.categ_proc_status_mob_no_vas
	,t1.ottok_last_month_mob_total
	,t1.ottok_last_month_mob_no_vas
	,t1.mark_zak_block
	,t1.month_zak_block
	,t1.metk_aff_ser
	
	,t1.flag_clc_mob_total
	,t1.flag_clc_mob_no_vas 
	,t1.flag_clc_mob_total_prev
	,t1.flag_clc_mob_no_vas_prev
	,t1.flag_clc_fclc_periodika
	,t1.flag_clc_fclc_periodika_prev
	,t1.flag_clc_fclc_bus_abon
	,t1.flag_clc_fclc_bus_abon_prev
	,t1.flag_clc_fclc_cat_number
	,t1.flag_clc_fclc_cat_number_prev
	
		
)with data distributed randomly;
analyze inserts_table;


insert into analit.table_analit_mob 
select * from inserts_table;
