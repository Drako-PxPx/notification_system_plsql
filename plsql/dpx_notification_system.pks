create or replace package dps_notification_system as

  TYPE r_reason_nbr IS RECORD
    ( left_op  number,
      right_op number,
      operator number,
      reason   varchar2(50)
    );
  TYPE t_reasons_nbr IS TABLE OF r_reason_nbr;
  c_op_equals        number := 1;
  c_op_greater_than  number := 2;
  c_op_less_than     number := 3;


function check_sql_stmt
  ( p_sqlstmt clob
  ) return number deterministic ;

function check_expr
    (p_expr varchar2
    ) return number deterministic ;


procedure eval_monitor
  ( p_monitor_id IN tinno_control.monitors.monitor_id%type,
    p_force      IN boolean DEFAULT FALSE
  );

TYPE t_rec_parameters IS RECORD
  ( parameter varchar2(30),
    value     varchar2(200)
  );

TYPE t_tab_parameters IS TABLE OF t_rec_parameters;

function parametrize_string
  ( p_input_string IN varchar2,
    p_parameters   IN varchar2
  ) return varchar2;

function foreign_getddl
  ( p_owner       IN varchar2,
    p_object_type IN varchar2,
    p_name        IN varchar2
  ) return clob;


procedure run;


/******************
 * EMail Features *
 ******************

  Following functions handles all email capabilities related to the notification
  system
*/

procedure add_recipient
  ( p_monitor_id IN tinno_control.monitors.monitor_id%type,
    p_recipient  IN varchar2
  );

/***************************
 * Hash Key Tables Feature *
 ***************************

  The following functions handles hash key tables, mainly used for the notification
  system transient information, but they  may be used for other processes as well,
  information is stored on notification_system_ht table.

  There are some cases where the information generation comes from a static SQL, just
  like a [materialized] view does, in this case the metadata information is being
  handled in monitor_ht_data_sets table.

  If there are processes that aren't meant to be handled by the notification system
  then the implementer might consider the option to move out these functions from
  this package, as each call will reinitialize the notification system which could
  lead to unneeded execution time and memory usage.
*/

  TYPE ht_row IS RECORD
    ( key    tinno_control.notification_system_ht.key%type,
      value  tinno_control.notification_system_ht.value%type
    );
  TYPE ht_table IS TABLE OF ht_row;

procedure create_ht_dataset
  ( p_monitor_id       IN number,
    p_data_set_name    IN varchar2,
    p_sqlstr           IN clob,
    p_header           IN varchar2  default 'Y',
    p_cached           IN varchar2  default 'N',
    p_expiration_mins  IN number    default null,
    p_skip_if_exists   IN varchar2  default 'Y'
  );

procedure compute_ht_dataset
  ( p_monitor_data_set_id in number
  );

function get_ds_id ( p_ds_name IN varchar2)
  return number;

function ht( p_domain IN varchar2)
  return ht_table pipelined;

procedure ht_add
  ( p_domain IN varchar2,
    p_key    IN varchar2,
    p_value  IN varchar2 DEFAULT NULL
  );

function display_dataset_prefix
  ( p_prefix IN varchar2 )
  return ht_table pipelined;

procedure flush_ht_dataset
  ( p_monitor_data_set_id  monitor_ht_data_sets.monitor_data_set_id%type
  );

procedure flush_ht
  ( p_domain IN varchar2
  );

procedure flush_ht_suffix
  ( p_suffix IN varchar2
  );

end dps_notification_system;
/