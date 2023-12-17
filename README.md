# utl-append-rows-and-merge-with-reference-table-in-sql-wps-r-and-python
Append rows and merge with reference table in sql wps r and python
    %let pgm=utl-append-rows-and-merge-with-reference-table-in-sql-wps-r-and-python;

    Append rows and merge with reference table in sql wps r and python

       SOLUTIONS

           1 wps sql
           2 wps r sql
           3 wps python sql
           4 wps r native
             https://stackoverflow.com/users/12993861/stefan


    github
    https://tinyurl.com/3w82bc66
    https://github.com/rogerjdeangelis/utl-append-rows-and-merge-with-reference-table-in-sql-wps-r-and-python

    stackoverflow r
    https://tinyurl.com/5mwyamdw
    https://stackoverflow.com/questions/77667714/sequentially-add-rows-according-to-shared-variable

    /*               _     _
     _ __  _ __ ___ | |__ | | ___ _ __ ___
    | `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
    | |_) | | | (_) | |_) | |  __/ | | | | |
    | .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
    |_|
    */

    /**************************************************************************************************************************/
    /*                                           |                                                                            */
    /*                    INPUTS                 |                            PROCESS                                         */
    /*=========== ============================== | ========================================================================   */
    /* TABLE REF                                 |                                                                            */
    /* SAMP VAL1 TABLE=HAVA  SAMP VAL2 VAL3 VAL4 | select                                                                     */
    /*                         a    3    5   9   |   *                                                                        */
    /*   a   1   TABLE=HAVB  SAMP VAL2 VAL3 VAL4 | from                                                                       */
    /*   b   2                 b    5    0   2   |   ref as l left join                                                       */
    /*   c   3   TABLE=HAVA  SAMP VAL2 VAL3 VAL4 | (select "a" as samp,3 as Val2,5 as Val3,9 as Val4 from hava union all      */
    /*   d   4                 c    7    1   3   |  select "b" as samp,5 as Val2,0 as Val3,2 as Val4 from havb union all      */
    /*   e   5                                   |  select "c" as samp,7 as Val2,1 as Val3,3 as Val4 from havc) as r          */
    /*                                           | on                                                                         */
    /*                                           |  l.samp  =  r.samp                                                         */
    /*                                                                                                                        */
    /*                              ----------------------------                                                              */
    /*                                                                                                                        */
    /*                                          OUTPUT                                                                        */
    /*                              ============================                                                              */
    /*                                                                                                                        */
    /*                               WANT  total obs=5                                                                        */
    /*                                                                                                                        */
    /*                               SAMP  VA1  VAL2  VAL3  VAL4                                                              */
    /*                                                                                                                        */
    /*                                a     1      3     5     9                                                              */
    /*                                b     2      5     0     2                                                              */
    /*                                c     3      7     1     3                                                              */
    /*                                d     4      .     .     .                                                              */
    /*                                e     5      .     .     .                                                              */
    /*                                           |                                                                            */
    /**************************************************************************************************************************/

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.ref;
    input
      SAMP $1. +1 VAL1;
    cards4;
    a 1
    b 2
    c 3
    d 4
    e 5
    ;;;;
    run;quit;

    data
      sd1.hava
      sd1.havb
      sd1.havc;
    samp="a"; Val2=3; Val3=5; Val4=9;output sd1.hava;
    samp="b"; Val2=5; Val3=0; Val4=2;output sd1.havb;
    samp="c"; Val2=7; Val3=1; Val4=3;output sd1.havc;
    stop;
    run;quit;

    /*                                  _
    / | __      ___ __  ___   ___  __ _| |
    | | \ \ /\ / / `_ \/ __| / __|/ _` | |
    | |  \ V  V /| |_) \__ \ \__ \ (_| | |
    |_|   \_/\_/ | .__/|___/ |___/\__, |_|
                 |_|                 |_|
    */

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    options validvarname=any;
    proc sql;
      create
        table sd1.want as
      select
        *
      from
        sd1.ref as l left join
      (select "a" as samp, 3 as val2, 5 as val3, 9 as val4 from sd1.hava union all
       select "b" as samp, 5 as val2, 0 as val3, 2 as val4 from sd1.havb union all
       select "c" as samp, 7 as val2, 1 as val3, 3 as val4 from sd1.havc) as r
      on
       l.samp  =  r.samp
    ;quit;
    ');

    proc print data=sd1.want;
    run;quit;


    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* Obs    SAMP    VA1    VALUE2    VALUE3    VALUE4                                                                       */
    /*                                                                                                                        */
    /*  1      a       1        3         5         9                                                                         */
    /*  2      b       2        5         0         2                                                                         */
    /*  3      c       3        7         1         3                                                                         */
    /*  4      d       4        .         .         .                                                                         */
    /*  5      e       5        .         .         .                                                                         */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    /*___                                          _
    |___ \  __      ___ __  ___   _ __   ___  __ _| |
      __) | \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
     / __/   \ V  V /| |_) \__ \ | |    \__ \ (_| | |
    |_____|   \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                     |_|                        |_|
    */

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.ref r=ref;
    export data=sd1.hava r=hava;
    export data=sd1.havb r=havb;
    export data=sd1.havc r=havc;
    submit;
    library(sqldf);
    want <- sqldf("
      select
        l.SAMP
       ,l.VAL1
       ,r.VAL2
       ,r.VAL3
       ,r.VAL4
      from
        ref as l left join
      (select \"a\" as SAMP, 3 as VAL2, 5 as VAL3, 9 as VAL4 from hava union all
       select \"b\" as SAMP, 5 as VAL2, 0 as VAL3, 2 as VAL4 from havb union all
       select \"c\" as SAMP, 7 as VAL2, 1 as VAL3, 3 as VAL4 from havc) as r
      on
       l.SAMP  =  r.SAMP
      ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    proc print data=sd1.want width=min;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS R System                                                                                                      */
    /*                                                                                                                        */
    /*    SAMP VAL1 VAL2 VAL3 VAL4                                                                                            */
    /*  1    a    1    3    5    9                                                                                            */
    /*  2    b    2    5    0    2                                                                                            */
    /*  3    c    3    7    1    3                                                                                            */
    /*  4    d    4   NA   NA   NA                                                                                            */
    /*  5    e    5   NA   NA   NA                                                                                            */
    /*                                                                                                                        */
    /*  The WPS System                                                                                                        */
    /*                                                                                                                        */
    /* Obs    SAMP    VAL1    VAL2    VAL3    VAL4                                                                            */
    /*                                                                                                                        */
    /*  1      a        1       3       5       9                                                                             */
    /*  2      b        2       5       0       2                                                                             */
    /*  3      c        3       7       1       3                                                                             */
    /*  4      d        4       .       .       .                                                                             */
    /*  5      e        5       .       .       .                                                                             */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*
     ____                                     _   _                             _
    |___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
      |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                     |_|         |_|    |___/                                |_|
    */

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    options validvarname=any lrecl=32756;
    libname sd1 'd:/sd1';
    proc python;
    export data=sd1.ref  python=ref;
    export data=sd1.hava python=hava;
    export data=sd1.havb python=havb;
    export data=sd1.havc python=havc;
    submit;
    import pyreadstat;
    from os import path;
    import pandas as pd;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    want = pdsql('''
      select
        l.SAMP
       ,l.VAL1
       ,r.VAL2
       ,r.VAL3
       ,r.VAL4
      from
        ref as l left join
      (select `a` as SAMP, 3 as VAL2, 5 as VAL3, 9 as VAL4 from hava union all
       select `b` as SAMP, 5 as VAL2, 0 as VAL3, 2 as VAL4 from havb union all
       select `c` as SAMP, 7 as VAL2, 1 as VAL3, 3 as VAL4 from havc) as r
      on
       l.SAMP  =  r.SAMP
    ''');
    print(want);
    pyreadstat.write_xport(want, 'd:/xpt/want.xpt',table_name='want',file_format_version=5);
    endsubmit;
    run;quit;
    ");

    libname xpt xport "d:/xpt/want.xpt";
    proc contents data=xpt._all_;
    run;quit;
    data want;
      set xpt.want;
    run;quit;
    proc print;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS PYTHON Procedure                                                                                               */
    /*                                                                                                                        */
    /*   SAMP  VAL1  VAL2  VAL3  VAL4                                                                                         */
    /* 0    a   1.0   3.0   5.0   9.0                                                                                         */
    /* 1    b   2.0   5.0   0.0   2.0                                                                                         */
    /* 2    c   3.0   7.0   1.0   3.0                                                                                         */
    /* 3    d   4.0   NaN   NaN   NaN                                                                                         */
    /* 4    e   5.0   NaN   NaN   NaN                                                                                         */
    /*                                                                                                                        */
    /* The WPS SYSTEM                                                                                                         */
    /*                                                                                                                        */
    /* Obs    SAMP    VAL1    VAL2    VAL3    VAL4                                                                            */
    /*                                                                                                                        */
    /*  1      a        1       3       5       9                                                                             */
    /*  2      b        2       5       0       2                                                                             */
    /*  3      c        3       7       1       3                                                                             */
    /*  4      d        4       .       .       .                                                                             */
    /*  5      e        5       .       .       .                                                                             */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*  _                                             _   _
    | || |   __      ___ __  ___   _ __   _ __   __ _| |_(_)_   _____
    | || |_  \ \ /\ / / `_ \/ __| | `__| | `_ \ / _` | __| \ \ / / _ \
    |__   _|  \ V  V /| |_) \__ \ | |    | | | | (_| | |_| |\ V /  __/
       |_|     \_/\_/ | .__/|___/ |_|    |_| |_|\__,_|\__|_| \_/ \___|
                      |_|
    */

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.ref r=ref;
    export data=sd1.hava r=hava;
    export data=sd1.havb r=havb;
    export data=sd1.havc r=havc;
    submit;
    library(dplyr, warn = FALSE);
    myinput <- bind_rows(hava, havb, havc);
    want<-left_join(ref, myinput, by = "SAMP");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    proc print data=sd1.want width=min;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* The WPS R System                                                                                                       */
    /*                                                                                                                        */
    /*   SAMP VAL1 VAL2 VAL3 VAL4                                                                                             */
    /* 1    a    1    3    5    9                                                                                             */
    /* 2    b    2    5    0    2                                                                                             */
    /* 3    c    3    7    1    3                                                                                             */
    /* 4    d    4   NA   NA   NA                                                                                             */
    /* 5    e    5   NA   NA   NA                                                                                             */
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /*    SAMP    VAL1    VAL2    VAL3    VAL4                                                                                */
    /*                                                                                                                        */
    /*     a        1       3       5       9                                                                                 */
    /*     b        2       5       0       2                                                                                 */
    /*     c        3       7       1       3                                                                                 */
    /*     d        4       .       .       .                                                                                 */
    /*     e        5       .       .       .                                                                                 */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
