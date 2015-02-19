/*
  
  Created on   : Oct 2014 by Mornin Feng
  Last updated : Oct 11 2014
 Extract data for paper for ICML with Marzyeh

*/



--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-------------------------- Statiic Variables -----------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

drop table mimic_data_nov14;
create table mimic_data_feb15 as
with population_1 as
(select distinct subject_id, hadm_id, icustay_id
from mimic2v26.icustay_detail
where icustay_seq=1 
and ICUSTAY_AGE_GROUP='adult'
and ICUSTAY_LOS>=48*60 -- at least 48 hour of icu stay
--and icustay_id<10
)

--select * from population; --15647

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-------------------------- Demographic and basic data  -----------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
, population_2 as
(select distinct
pop.*
, round(icud.icustay_los/60/24, 2) as icu_los_day
, round(icud.hospital_los/60/24,2) as hospital_los_day
, case when icud.icustay_admit_age>120 then 91.4 else  icud.icustay_admit_age end as age
--, icud.gender as gender
, case when icud.gender is null then null
  when icud.gender = 'M' then 1 else 0 end as gender_num
, icud.WEIGHT_FIRST
, bmi.bmi
, bmi.IMPUTED_INDICATOR
, icud.SAPSI_FIRST
, icud.SOFA_FIRST
, icud.ICUSTAY_FIRST_SERVICE as service_unit
, case when ICUSTAY_FIRST_SERVICE='SICU' then 1
      when ICUSTAY_FIRST_SERVICE='CCU' then 2
      when ICUSTAY_FIRST_SERVICE='CSRU' then 3
      else 0 --MICU & FICU
      end
  as service_num
, icud.icustay_intime 
, icud.icustay_outtime
, to_char(icud.ICUSTAY_INTIME, 'Day') as day_icu_intime
, to_number(to_char(icud.ICUSTAY_INTIME, 'D')) as day_icu_intime_num
, extract(hour from icud.ICUSTAY_INTIME) as hour_icu_intime
, case when icud.hospital_expire_flg='Y' then 1 else 0 end as hosp_exp_flg
, case when icud.icustay_expire_flg='Y' then 1 else 0 end as icu_exp_flg
, round((extract(day from d.dod-icud.icustay_intime)+extract(hour from d.dod-icud.icustay_intime)/24),2) as survival_day
from population_1 pop 
left join  mimic2v26.icustay_detail icud on pop.icustay_id = icud.icustay_id
left join mimic2devel.obesity_bmi bmi on bmi.icustay_id=pop.icustay_id
left join MIMIC2DEVEL.d_patients d on d.subject_id=pop.subject_id
)

--select * from population_2;

, population as
(select distinct pop.*
, elix.CONGESTIVE_HEART_FAILURE
, elix.CARDIAC_ARRHYTHMIAS
, elix.VALVULAR_DISEASE
, elix.PULMONARY_CIRCULATION
, elix.PERIPHERAL_VASCULAR
, elix.HYPERTENSION
, elix.PARALYSIS
, elix.OTHER_NEUROLOGICAL
, elix.CHRONIC_PULMONARY
, elix.DIABETES_UNCOMPLICATED
, elix.DIABETES_COMPLICATED
, elix.HYPOTHYROIDISM
, elix.RENAL_FAILURE
, elix.LIVER_DISEASE
, elix.PEPTIC_ULCER
, elix.AIDS
, elix.LYMPHOMA
, elix.METASTATIC_CANCER
, elix.SOLID_TUMOR
, elix.RHEUMATOID_ARTHRITIS
, elix.COAGULOPATHY
, elix.OBESITY
, elix.WEIGHT_LOSS
, elix.FLUID_ELECTROLYTE
, elix.BLOOD_LOSS_ANEMIA
, elix.DEFICIENCY_ANEMIAS
, elix.ALCOHOL_ABUSE
, elix.DRUG_ABUSE
, elix.PSYCHOSES
, elix.DEPRESSION
, pt.HOSPITAL_MORT_PT as elix_HOSPITAL_MORT_PT
, pt.TWENTY_EIGHT_DAY_MORT_PT as elix_TWENTY_EIGHT_DAY_MORT_PT
, pt.ONE_YR_MORT_PT as pt_ONE_YR_MORT_PT
, pt.TWO_YR_MORT_PT as pt_TWO_YR_MORT_PT
, pt.ONE_YEAR_SURVIVAL_PT as pt_ONE_YEAR_SURVIVAL_PT
, pt.TWO_YEAR_SURVIVAL_PT as pt_TWO_YEAR_SURVIVAL_PT
from population_2 pop
left join mimic2devel.elixhauser_revised elix on elix.hadm_id=pop.hadm_id
left join mimic2devel.ELIXHAUSER_POINTS pt on pt.hadm_id=pop.hadm_id
)

--select * from population;
, temp as
(select ICUSTAY_ID
,ICU_LOS_DAY
,HOSPITAL_LOS_DAY
,AGE
,GENDER_NUM
,WEIGHT_FIRST
,BMI
,case when IMPUTED_INDICATOR = 'FALSE' then 0 
      when IMPUTED_INDICATOR = 'TRUE' then 1
      else null end as IMPUTED_INDICATOR
,SAPSI_FIRST
,SOFA_FIRST
--,SERVICE_UNIT
,SERVICE_NUM
--,ICUSTAY_INTIME
--,ICUSTAY_OUTTIME
--,DAY_ICU_INTIME
,DAY_ICU_INTIME_NUM
,HOUR_ICU_INTIME
,HOSP_EXP_FLG
,ICU_EXP_FLG
,SURVIVAL_DAY
,CONGESTIVE_HEART_FAILURE
,CARDIAC_ARRHYTHMIAS
,VALVULAR_DISEASE
,PULMONARY_CIRCULATION
,PERIPHERAL_VASCULAR
,HYPERTENSION
,PARALYSIS
,OTHER_NEUROLOGICAL
,CHRONIC_PULMONARY
,DIABETES_UNCOMPLICATED
,DIABETES_COMPLICATED
,HYPOTHYROIDISM
,RENAL_FAILURE
,LIVER_DISEASE
,PEPTIC_ULCER
,AIDS
,LYMPHOMA
,METASTATIC_CANCER
,SOLID_TUMOR
,RHEUMATOID_ARTHRITIS
,COAGULOPATHY
,OBESITY
,WEIGHT_LOSS
,FLUID_ELECTROLYTE
,BLOOD_LOSS_ANEMIA
,DEFICIENCY_ANEMIAS
,ALCOHOL_ABUSE
,DRUG_ABUSE
,PSYCHOSES
,DEPRESSION
,ELIX_HOSPITAL_MORT_PT
,ELIX_TWENTY_EIGHT_DAY_MORT_PT
,PT_ONE_YR_MORT_PT
,PT_TWO_YR_MORT_PT
,PT_ONE_YEAR_SURVIVAL_PT
,PT_TWO_YEAR_SURVIVAL_PT
from population
)

--select * from temp;
, temp1 as
(
select * from temp
unpivot (value1 for variable_name 
in (
ICU_LOS_DAY as 'ICU_LOS_DAY'
,HOSPITAL_LOS_DAY as 'HOSPITAL_LOS_DAY'
,AGE as 'AGE'
,GENDER_NUM as 'GENDER_MALE'
,WEIGHT_FIRST as 'WEIGHT_FIRST'
,BMI as 'BMI'
,IMPUTED_INDICATOR as 'BMI_IMPUTED_INDICATOR'
,SAPSI_FIRST as 'SAPSI_FIRST'
,SOFA_FIRST as 'SOFA_FIRST'
--,SERVICE_UNIT as 
,SERVICE_NUM as 'SERVICE_UNIT_NUM'
--,ICUSTAY_INTIME as 'ICUSTAY_INTIME'
--,ICUSTAY_OUTTIME as 'ICUSTAY_OUTTIME'
--,DAY_ICU_INTIME as 'DAY_ICU_INTIME'
,DAY_ICU_INTIME_NUM as 'DAY_ICU_INTIME_NUM'
,HOUR_ICU_INTIME as 'HOUR_ICU_INTIME'
,HOSP_EXP_FLG as 'HOSP_EXP_FLG'
,ICU_EXP_FLG as 'ICU_EXP_FLG'
,SURVIVAL_DAY as 'SURVIVAL_DAY'
,CONGESTIVE_HEART_FAILURE as 'CONGESTIVE_HEART_FAILURE'
,CARDIAC_ARRHYTHMIAS as 'CARDIAC_ARRHYTHMIAS'
,VALVULAR_DISEASE as 'VALVULAR_DISEASE'
,PULMONARY_CIRCULATION as 'PULMONARY_CIRCULATION'
,PERIPHERAL_VASCULAR as 'PERIPHERAL_VASCULAR'
,HYPERTENSION as 'HYPERTENSION'
,PARALYSIS as 'PARALYSIS'
,OTHER_NEUROLOGICAL as 'OTHER_NEUROLOGICAL'
,CHRONIC_PULMONARY as 'CHRONIC_PULMONARY'
,DIABETES_UNCOMPLICATED as 'DIABETES_UNCOMPLICATED'
,DIABETES_COMPLICATED as 'DIABETES_COMPLICATED'
,HYPOTHYROIDISM as 'HYPOTHYROIDISM'
,RENAL_FAILURE as 'RENAL_FAILURE'
,LIVER_DISEASE as 'LIVER_DISEASE'
,PEPTIC_ULCER as 'PEPTIC_ULCER'
,AIDS as 'AIDS'
,LYMPHOMA as 'LYMPHOMA'
,METASTATIC_CANCER as 'METASTATIC_CANCER'
,SOLID_TUMOR as 'SOLID_TUMOR'
,RHEUMATOID_ARTHRITIS as 'RHEUMATOID_ARTHRITIS'
,COAGULOPATHY as 'COAGULOPATHY'
,OBESITY as 'OBESITY'
,WEIGHT_LOSS as 'WEIGHT_LOSS'
,FLUID_ELECTROLYTE as 'FLUID_ELECTROLYTE'
,BLOOD_LOSS_ANEMIA as 'BLOOD_LOSS_ANEMIA'
,DEFICIENCY_ANEMIAS as 'DEFICIENCY_ANEMIAS'
,ALCOHOL_ABUSE as 'ALCOHOL_ABUSE'
,DRUG_ABUSE as 'DRUG_ABUSE'
,PSYCHOSES as 'PSYCHOSES'
,DEPRESSION as 'DEPRESSION'
,ELIX_HOSPITAL_MORT_PT as 'ELIX_HOSPITAL_MORT_PT'
,ELIX_TWENTY_EIGHT_DAY_MORT_PT as 'ELIX_TWENTY_EIGHT_DAY_MORT_PT'
,PT_ONE_YR_MORT_PT as 'ELIX_ONE_YR_MORT_PT'
,PT_TWO_YR_MORT_PT as 'ELIX_TWO_YR_MORT_PT'
,PT_ONE_YEAR_SURVIVAL_PT as 'ELIX_ONE_YEAR_SURVIVAL_PT'
,PT_TWO_YEAR_SURVIVAL_PT as 'EKIX_TWO_YEAR_SURVIVAL_PT'
))
)

, population1 as
(select icustay_id,  variable_name, 0 as timestamp_hr, value1, to_char(null) as value1uom
from temp1
)

--select * from population1;




--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-------------------------- Spare time series Variables -------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------



--------------------------------------------------------------------------------------------------------
-------------------------- Vent & vasopressor patients  ------------------------------------------------
--------------------------------------------------------------------------------------------------------
--with population as
--(select *
--from challenge2012_static_variables
--where icustay_id<10
--)

--select * from population;

, vent_group as
(select distinct 
pop.icustay_id
, 'ventilation' as variable_name
--, ch.charttime-pop.icustay_intime
, round( (extract(day from ch.charttime-pop.icustay_intime) *24
    + extract(hour from ch.charttime-pop.icustay_intime)
    + extract(minute from ch.charttime-pop.icustay_intime)/60
  ), 2)as timestamp_hr
--, vent.end_time as timestamp_2
, 1 as value1
, 'flag' as value1uom
from population pop
join mimic2v26.chartevents ch 
  on ch.icustay_id=pop.icustay_id 
  and ch.itemid in (720,722)
)

--select * from vent_group;

--- unit unified based on percentage of recommended max dosage

, vaso_group as
(select 
distinct 
pop.icustay_id
, 'vasopressor' as variable_name
, round( (extract(day from med.charttime-pop.icustay_intime) *24
    + extract(hour from med.charttime-pop.icustay_intime)
    + extract(minute from med.charttime-pop.icustay_intime)/60
  ), 2)as timestamp_hr
--, null as timestamp_2
, case when med.itemid=51 then round(med.dose/0.03*100,3)
      when med.itemid=43 then round(med.dose/15*100,3)
      when med.itemid=119 then round(med.dose/0.125*100,3)
      when med.itemid=120 then round(med.dose/3*100,3)
      when med.itemid=128 then round(med.dose/9.1*100,3)
    end as value1
, '% of max dosage' as value1uom
--, med.itemid
from population pop
join mimic2v26.medevents med on med.icustay_id=pop.icustay_id 
  and med.itemid in (51,43,128,120, 119) --- a more concise list
  and med.dose>0
  --and med.itemid in (46,47,120,43,307,44,119,309,51,127,128)
where med.charttime is not null
)

--select distinct itemid, count(*) from vaso_group group by itemid;
--select * from vaso_group;


---- 
--- Fentanyl 50 mcg/hr
--- 
, sedative_group as
(select 
distinct 
pop.icustay_id
, 'sedative' as variable_name
, round( (extract(day from med.charttime-pop.icustay_intime) *24
    + extract(hour from med.charttime-pop.icustay_intime)
    + extract(minute from med.charttime-pop.icustay_intime)/60
  ), 2)as timestamp_hr
--, null as timestamp_2
, 1 as value1
, 'flg' as value1uom
--, med.itemid
from population pop
join mimic2v26.medevents med on med.icustay_id=pop.icustay_id 
  --and med.itemid in (124,118,149,150,308,163,131)
  and itemid in (149,163,131,124,118)
where med.charttime is not null
)

--select count(distinct icustay_id) from sedative_group;
--select distinct itemid, value1uom from sedative_group;
--select * from sedative_group;

--------------------------------------------------------------------------------------------------------
-------------------------- Chart events/ vital signs   ---------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
, chart_data1 as
(select distinct
pop.icustay_id
, case when ch.itemid in (52,456) then 'MeanBP'
      when ch.itemid in (678,679) then 'Temperature F'
      when ch.itemid =211 then 'HR'
      when ch.itemid =113 then 'CVP'
      when ch.itemid =646 then 'SPO2'
      when ch.itemid in (190,3420) then 'FIO2'
      when  ch.itemid =198 then 'GCS'
      --when  ch.itemid =128 then 'Care_Protocol'
      when  ch.itemid =3580 then 'weight_kg'
      when ch.itemid =619 then 'ventilated_RR'
      when ch.itemid in (614,615,618) then 'spontaneous_RR'
    end as variable_name
, round( (extract(day from ch.charttime-pop.icustay_intime) *24
    + extract(hour from ch.charttime-pop.icustay_intime)
    + extract(minute from ch.charttime-pop.icustay_intime)/60
  ), 2)as timestamp_hr
--, null as timestamp_2
, round(ch.value1num, 3) as value1
, case when ch.itemid in (190,3420) then 'fraction'
    else ch.value1uom end as value1uom
from population pop
join mimic2v26.chartevents ch 
  on pop.icustay_id=ch.icustay_id 
    --and ch.charttime <= pop.icustay_intime+3
where (ch.itemid in (52,456) -- mean bp
    or ch.itemid in (678,679)  -- temperature in F
    or ch.itemid =211 -- hr
    or ch.itemid =113 -- cvp
    or ch.itemid =646 -- spo2
    or ch.itemid in (190,3420) -- fio2
    or ch.itemid =198 -- GCS
    --or ch.itemid=128 -- care protocol
    or ch.itemid=3580 -- weight_kg
    or ch.itemid =619 -- ventilated_RR
    or ( ch.itemid in (614,615,618) and ch.value1num between 2 and 80))-- spontaneous_RR
    and ch.value1num is not null
)

--select * from chart_data1 where value1 is null;

, chart_data2 as
(select distinct
pop.icustay_id
,  'Care_Protocol' as variable_name
, round( (extract(day from ch.charttime-pop.icustay_intime) *24
    + extract(hour from ch.charttime-pop.icustay_intime)
    + extract(minute from ch.charttime-pop.icustay_intime)/60
  ), 2)as timestamp_hr
, case when value1 = 'Full Code' then 0
      when value1 = 'Comfort Measures' then 1
      when value1 = 'Do Not Intubate' then 2
      when value1 = 'Do Not Resuscita' then 3
      when value1 = 'CPR Not Indicate' then 4
      when value1 = 'Other/Remarks' then 5
      else null
    end as value1
, null as value1uom
from population pop
join mimic2v26.chartevents ch 
  on pop.icustay_id=ch.icustay_id 
  and ch.itemid=128 -- care protocol
  and ch.value1 is not null
)

--select * from chart_data2;

, chart_data as
(
select * from chart_data1
union
select * from chart_data2
)

--select * from chart_data;
--------------------------------------------------------------------------------------------------------
-------------------------- Lab results   ---------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
, lab_data as
(select distinct
pop.icustay_id
, case when lab.itemid = 50060 then 'Albumin' 
  when lab.itemid = 50061 then 'ALP' 
  when lab.itemid = 50062 then 'ALT'
  when lab.itemid = 50073 then 'AST'
  --when lab.itemid = 50626 then 'Bilirubin'
  when lab.itemid = 50177 then 'BUN'
  when lab.itemid = 50085 then 'cholesterol'
  when lab.itemid = 50090 then 'creatinine'
  when lab.itemid in (50006,50112)  then 'glucose'
  when lab.itemid in (50172, 50025,50022)  then 'bicarbonate'
  when lab.itemid in (50029,50383) then 'HCT'
  when lab.itemid = 50010 then 'lactate'
  when lab.itemid = 50140 then 'magnesium'
  when lab.itemid = 50016 then 'paco2'
  when lab.itemid = 50018 then 'ph'
  when lab.itemid = 50428 then 'platelets'
  when lab.itemid = 50019 then 'po2'
  when lab.itemid in (50149, 50009) then 'potassium'
  when lab.itemid = 50015 then 'sao2'
  when lab.itemid in (50159, 50012) then 'sodium'
  when lab.itemid = 50188 then 'TropI'
  when lab.itemid = 50189 then 'TropT'
  when lab.itemid in (50316,50468) then 'WBC'
  end as variable_name
, round( (extract(day from lab.charttime-pop.icustay_intime) *24
    + extract(hour from lab.charttime-pop.icustay_intime)
    + extract(minute from lab.charttime-pop.icustay_intime)/60
  ), 2)as timestamp_hr
--, null as timestamp_2
, lab.valuenum as value1
, lab.valueuom as value1uom
from population pop
join mimic2v26.labevents lab on lab.icustay_id=pop.icustay_id and lab.valuenum is not null
    --and ch.charttime <= pop.icustay_intime+3
where lab.itemid = 50060 -- Albumin
  or lab.itemid = 50061 -- ALP
  or lab.itemid = 50062 -- ALT
  or lab.itemid = 50073 -- AST
  --or lab.itemid = 50626 -- Bilirubin
  or lab.itemid = 50177 -- bun
  or lab.itemid = 50085 -- cholesterol
  or lab.itemid = 50090 -- creatinine
  or lab.itemid in (50006,50112) --glucose
  or lab.itemid in (50172, 50025,50022) -- bicarbonate
  or lab.itemid in (50029,50383) --HCT
  or lab.itemid = 50010 -- lactate
  or lab.itemid = 50140 -- magnesium
  or lab.itemid = 50016 --paco2
  or lab.itemid = 50018 --ph 
  or lab.itemid = 50428 -- platelets
  or lab.itemid = 50019 --po2 
  or lab.itemid in (50149, 50009)  -- potassium
  or lab.itemid = 50015 -- sao2
  or lab.itemid in (50159, 50012) -- sodium 
  or lab.itemid = 50188 -- TropI
  or lab.itemid = 50189 -- TropT
  or lab.itemid in (50316,50468) -- WBC 
)

--select * from lab_data;

, bilirubin_data as
(select distinct
pop.icustay_id
, 'Bilirubin' as variable_name
, round( (extract(day from lab.charttime-pop.icustay_intime) *24
    + extract(hour from lab.charttime-pop.icustay_intime)
    + extract(minute from lab.charttime-pop.icustay_intime)/60
  ), 2)as timestamp_hr
--, null as timestamp_2
, case when lab.value in ('NEG', 'N', 'Neg') then 0
       when lab.value ='LG' then 1 
       when lab.value ='SM' then 2
       else 3
       end as value1
, to_char(null) as value1uom
from population pop
join mimic2v26.labevents lab on lab.icustay_id=pop.icustay_id
    --and ch.charttime <= pop.icustay_intime+3
where  lab.itemid = 50626 -- Bilirubin
  
)

--select * from bilirubin_data;
--------------------------------------------------------------------------------------------------------
-------------------------- Final integration  ----------------------------------------------------------
--------------------------------------------------------------------------------------------------------

, time_series_table as
(
select * from population1
union
select * from vent_group
union
select * from vaso_group
union
select * from sedative_group
union
select * from chart_data
union
select * from lab_data
union
select * from bilirubin_data
)

select * from time_series_table order by 1;

--select * from time_series_table where variable_name ='Bilirubin' order by 1;

-------- exporting to csv file

--- need to run as a script
spool "/home/mornin/Dropbox/MIT/data/test.csv"

select /*csv*/ * from MIMIC_DATA_NOV14;

spool off;
