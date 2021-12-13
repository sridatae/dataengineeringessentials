%let DEBUG = N;

/*************Start a CAS Session*************/ 
cas mySession sessopts=(caslib=casuser timeout-1800 locale="en_US");

/*Assign all caslibs */
Caslib_ALL_ assign;
-----------------------------------------------------------------------------------------------
%macro ar_to_chicago;
/*
Acceptance Date

End Date
*/
proc casutil;
load casdata="broker_info.sashdat" incaslib="STG_SnB" outcaslib="CASUSER"
     casout="broker_info" replace;
load casdata="accountfinancials_final.sashdat” incaslib=”STG_SnB” outcaslib=”CASUSER” 
      casout=”accountfinancials_final” replace;
load casdata=”contract_info_first_final.sashdat” incaslib=”STG_SnB” outcaslib=”CASUSER” 
      casout=”contract_info_first_final” replace; 
load casdata=”rate_info_final.sashdat” incaslib=”STG SnB” outcaslib=”CASUSER”
      casout=”rate_info_final” replace;
load casdata=“booked_eff_date_accounts_final.sashdat” incaslib=“STG_SnB” outcaslib=”CASUSER”
      casout=”booked_eff_date_accounts final” replace;
load casdata=“balance info.sashdat” incaslib=“STG SnB” outcaslib=”CASUSER”
    casout=“balance info” replace;
load casdata=”client_info_final.sashdat” incaslib=”STG SnB” outcaslib-“CASUSER”
      casout-“client_info_final” replace;
load casdata=“arrears_info.sashdat” incaslib=”STG_SnB” outcaslib=”CASUSER” 
      casout-“arrears_info” replace;
run;

PROC FEDSQL sessref=mySession label;

CREATE TABLE CASUSER.AR_to_chicago (option replace=True) AS
SELECT t1.Account Number,
      T4.Client AS Customer,
      T1.AmountFinanced AS NetFinanced,
      T2.BrokerageName,
      T3.BookedDate,
      T3.EffectiveDate,
      T3.OriginalFundingDate,
      T3.ModifiedOriginal FundingDate,
      T5.Accounts Receivable AS AR Balance,
      T6.APR,
      T7.Account Status,
      T7.Currency,
      T7.PortfolioName AS Portfolio,
      T7. Term,
      T7.BrokerId,
      T8.ArrearsDays,
put(intnx('month’, datepart (t3. EffectiveDate), t7.term, ‘same’), date9.) AS END_Date
FROM CASUSER.contract info first final t7
      LEFT JOIN CASUSER.account financials_final t1 ON (t7.Account Number = t1.Account Number)
    LEFT JOIN CASUSER. Rate info_final t6 ON (t7.Account Number = t6.Account Number)
    LEFT JOIN CASUSER.client_info_final t4 ON (t7.Account Number = t4.Account Number)
    LEFT JOIN CASUSER. Booked_eff_date_accounts final t3 ON (t7.Account Number = t3.Account Number) 
    LEFT JOIN CASUSER.broker_info t2 ON (t7.BrokerId = t2.BrokerId)
    LEFT JOIN CASUSER.balance_info t5 ON (t7.Account Number = t5.Account Number) 
    LEFT JOIN CASUSER.arrears_info t8 ON (t7.Account Number = t8.Account Number)
    WHERE abs (t5.AR_Amount) > 0.01 ;
QUIT;

%if &sqlrc> 4 %then %do;
  %put [ERROR: &SYSMACRONAME]: PROC FEDSQL Step Failed;
  %goto ERROR;
%end;

/*Save the in-memory table to disk*/

%if &debug = N %then %do; 
    proc casutil incaslib=”CASUSER” outcaslib=”VA_SnB”;
      save casdata=”AR_to_chicago” replace;
    run;

  %put Dropping/Unload table from memory (VA_SnB)….;
    proc casutil;
      droptable casdata=”AR_to_chicago” incaslib=”VA_SnB” QUIET;
    run;
  %put Loading table back into memory (VA_SnB)….; 
    proc casutil;
      load casdata=”AR_to_chicago.sashdat” incaslib=”VA_SnB” outcaslib=”VA_SnB” 
      casout=”AR to chicago” promote;
    run; 
%end;

Load casdata=”AR_to_chicago.sashdat” incaslib=”VA_SnB” outcaslib=”VA_SnB” casout=”AR_to_chicago” promote;

%goto SUCCESS;

%SUCCESS:
  %put [INFO:&SYSMACRONAME]: Macro completed successfully;
  %goto DONE;
%ERROR:
  %put [INFO:&SYSMACRONAME]: Macro completed with errors;
  %let syscc = 9;
  %goto DONE;

%DONE:

%mend;

%ar_to_chicago;

