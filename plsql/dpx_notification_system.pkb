create or replace package body dps_notification_system as

  gl_setup_set notification_system_parameters.parameter_value%type;

function getsize
  (p_var in out nocopy varchar2)
return number as
  l_retval number;
begin
  l_retval := anydata.convertchar(nvl(p_var,' ')).getchar(p_var);
  l_retval := length(p_var);
  return l_retval;
end getsize;

function check_sql_stmt
  ( p_sqlstmt clob
  ) return number AS
  l_cursor number;
  l_retval number := 1;
begin
  l_cursor := dbms_sql.OPEN_CURSOR();

  dbms_sql.parse(l_cursor,p_sqlstmt,dbms_sql.NATIVE);

  dbms_sql.CLOSE_CURSOR(l_cursor);
  return l_retval;
exception
  when others then
    l_retval := 0;
    dbms_output.put_line(sqlerrm);
    return l_retval;
end check_sql_stmt;

function check_expr
    (p_expr varchar2
    ) return number AS
begin
  return check_sql_stmt('SELECT count(0) FROM dual WHERE ' || p_expr);
end check_expr;

procedure send_email
  ( p_rcpt_list IN dbms_sql.varchar2_table,
    p_subject   IN varchar2,
    p_message   IN varchar2,
    p_attachment_name IN varchar2 default null,
    p_attachment_mime IN varchar2 default null,
    p_attachment_content IN blob default null
  ) AS
  TYPE trec_smtp_parameters IS RECORD
   ( smtp_server    notification_system_parameters.parameter_value%type,
     smtp_port      notification_system_parameters.parameter_value%type,
     smtp_sender    notification_system_parameters.parameter_value%type,
     smtp_username  notification_system_parameters.parameter_value%type,
     smtp_password  notification_system_parameters.parameter_value%type
   );
  c_boundary         constant varchar2(50) := '----=*#abc1234321cba#*=';
  l_step                      pls_integer := 12000;
  l_smtp_parameters           trec_smtp_parameters;
  l_mail_conn                 utl_smtp.connection;
  l_to_list                   varchar2(2000);
begin
  -- get configuration from notification_system_parameters
  select *
    into l_smtp_parameters
    from ( select parameter_name, parameter_value
             from notification_system_parameters
            where parameter_name in ( 'SMTP Host',
                                      'SMTP Port',
                                      'SMTP Sender',
                                      'SMTP Username',
                                      'SMTP Password'
                                    )
              and setup_set = gl_setup_set
         )
   pivot ( max(parameter_value)
           for parameter_name in ( 'SMTP Host'      as SMTP_Host,
                                   'SMTP Port'      as SMTP_Port,
                                   'SMTP Sender'    as SMTP_Sender,
                                   'SMTP Username'  as SMTP_Username,
                                   'SMTP Password'  as SMTP_Password
                                 )
         );

  l_mail_conn := utl_smtp.open_connection
    ( host => l_smtp_parameters.smtp_server,
      port => to_number(l_smtp_parameters.smtp_port)
    );
  utl_smtp.starttls(l_mail_conn);
  UTL_SMTP.AUTH( c        => l_mail_conn,
                 username => l_smtp_parameters.smtp_username,
                 password => l_smtp_parameters.smtp_password,
                 schemes  => 'PLAIN'
               );
  utl_smtp.helo(l_mail_conn,l_smtp_parameters.smtp_server);
  utl_smtp.mail(l_mail_conn,l_smtp_parameters.smtp_sender);

  for i in p_rcpt_list.first .. p_rcpt_list.last loop
    utl_smtp.rcpt(l_mail_conn,p_rcpt_list(i));
    if (trim(l_to_list) is not null ) then
      l_to_list := l_to_list || '; ';
    end if;
    l_to_list := l_to_list || p_rcpt_list(i);
  end loop;
  utl_smtp.open_data(l_mail_conn);
  utl_smtp.write_data(l_mail_conn,'Date: ' || to_char(sysdate,'DD-MON-YYYY HH24:MI:SS') || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn,'To: ' || l_to_list || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn,'From: ' || l_smtp_parameters.smtp_sender || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn,'Subject: ' || p_subject || utl_tcp.crlf);
  --utl_smtp.write_raw_data(l_mail_conn,utl_raw.cast_to_raw('Subject: ' || p_subject || utl_tcp.crlf));
  utl_smtp.write_data(l_mail_conn,'MIME-Version: 1.0' || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn,'Content-Type: multipart/mixed; boundary="' || c_boundary || '"' || utl_tcp.crlf || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn,'--' || c_boundary || utl_tcp.crlf);

  -- Message start
  utl_smtp.write_data(l_mail_conn,'Content-Type: text/plain; charset="is-8859-1"' || utl_tcp.crlf || utl_tcp.crlf);
  utl_smtp.write_data(l_mail_conn,p_message);
  utl_smtp.write_data(l_mail_conn,utl_tcp.crlf || utl_tcp.crlf);
  -- Message end

  if (p_attachment_name is not null) then
    utl_smtp.write_data(l_mail_conn,'--' || c_boundary || utl_tcp.crlf);
    utl_smtp.write_data(l_mail_conn,'Content-Type: ' || p_attachment_mime || '; name="' || p_attachment_name || '"' || utl_tcp.crlf);
    utl_smtp.write_data(l_mail_conn,'Content-Transfer-Encoding: base64' || utl_tcp.crlf);
    utl_smtp.write_data(l_mail_conn,'Content-Disposition: attachment; filename="' || p_attachment_name || '"' || utl_tcp.crlf || utl_tcp.crlf);
    for i in 0 .. trunc ((dbms_lob.getlength(p_attachment_content) -1)/l_step) loop
      utl_smtp.write_data(l_mail_conn, utl_raw.cast_to_varchar2(utl_encode.base64_encode(dbms_lob.substr(p_attachment_content, l_step, i*l_step+1))) || utl_tcp.crlf);
    end loop;

  end if;

  utl_smtp.close_data(l_mail_conn);
  utl_smtp.quit(l_mail_conn);
EXCEPTION
  WHEN UTL_smtp.transient_error OR UTL_smtp.permanent_error THEN
    UTL_smtp.quit(l_mail_conn);
    dbms_output.put_line(sqlerrm);
    raise;
end send_email;

/* This function will evaluate the given condition from the SQL statement:
  - The SQL statement should return 1 row only
*/
function sql_condition_check
  ( p_cursor IN OUT NOCOPY number ,
    p_condition IN varchar2
  ) return number AS

  TYPE t_values IS TABLE OF SYS.ANYDATA;

  --l_cursor       number;
  l_expr_cursor  number;
  l_ncols        pls_integer;
  l_coldefs      dbms_sql.desc_tab2;
  l_values       t_values;

  l_vc2_value  varchar2(4000);
  l_nbr_value  number;
  l_dt_value   date;
  l_retval     number;
begin
  l_values := new t_values();
  --l_cursor := dbms_sql.open_cursor;
  --dbms_sql.parse(l_cursor,p_sqlcmd,dbms_sql.native);
  dbms_sql.describe_columns2(p_cursor,l_ncols,l_coldefs);

  -- Definition of all column types
  for i in 1 .. l_ncols loop
    case
      when l_coldefs(i).col_type in (1,96) then
        dbms_sql.define_column(p_cursor,i,l_vc2_value,4000);
      when l_coldefs(i).col_type = 2 then
        dbms_sql.define_column(p_cursor,i,l_nbr_value);
      when l_coldefs(i).col_type = 12 then
        dbms_sql.define_column(p_cursor,i,l_dt_value);
    end case;
  end loop;

  -- Fetch column values
  l_nbr_value := dbms_sql.execute_and_fetch(p_cursor,TRUE);
  for i in 1 .. l_ncols loop
    l_values.extend;
    case
      when l_coldefs(i).col_type in (1,96) then
        dbms_sql.column_value(p_cursor,i,l_vc2_value);
        l_values(l_values.last) := sys.anydata.convertVarchar2(l_vc2_value);
      when l_coldefs(i).col_type = 2 then
        dbms_sql.column_value(p_cursor,i,l_nbr_value);
        l_values(l_values.last) := sys.anydata.convertNumber(l_nbr_value);
      when l_coldefs(i).col_type = 12 then
        dbms_sql.column_value(p_cursor,i,l_dt_value);
        l_values(l_values.last) := sys.anydata.convertDate(l_dt_value);
    end case;
  end loop;

  l_expr_cursor := dbms_sql.open_cursor;
  dbms_sql.parse(l_expr_cursor,'SELECT count(0) from dual WHERE ' || p_condition,dbms_sql.native);
  for i in 1 .. l_ncols loop
    case
      when l_coldefs(i).col_type in (1,96) then
        l_vc2_value := sys.anydata.accessVarchar2(l_values(i));
        dbms_sql.bind_variable(l_expr_cursor,':' || i, l_vc2_value);

      when l_coldefs(i).col_type= 2 then
        l_nbr_value := sys.anydata.accessNumber(l_values(i));
        dbms_sql.bind_variable(l_expr_cursor,':' || i, l_nbr_value);

      when l_coldefs(i).col_type = 12 then
        l_dt_value := sys.anydata.accessDate(l_values(i));
        dbms_sql.bind_variable(l_expr_cursor,':' || i, l_dt_value);
      end case;
  end loop;
  dbms_sql.define_column(l_expr_cursor,1,l_retval);
  l_retval := dbms_sql.EXECUTE_AND_FETCH(l_expr_cursor,TRUE);
  dbms_sql.column_value(l_expr_cursor,1,l_retval);
  dbms_sql.close_cursor(l_expr_cursor);

  l_values.delete;

  dbms_sql.close_cursor(p_cursor);
  return l_retval;
exception
  when others then
    if (dbms_sql.IS_OPEN(p_cursor)) then
      dbms_sql.close_cursor(p_cursor);
    end if;
    if (dbms_sql.is_open(l_expr_cursor)) then
      dbms_sql.close_cursor(l_expr_cursor);
    end if;
    raise;
end sql_condition_check;

function sql_condition_check
  ( p_sqlcmd    IN clob,
    p_condition IN varchar2
  ) return number as
  l_cursor number;
begin
  l_cursor := dbms_sql.open_cursor;
  dbms_sql.parse(l_cursor,p_sqlcmd,dbms_sql.native);
  return sql_condition_check(l_cursor,p_condition);
end sql_condition_check;

procedure eval_monitor
  ( p_monitor_id IN monitors.monitor_id%type,
    p_force      IN boolean DEFAULT FALSE
  ) as
  l_receipts          dbms_sql.varchar2_Table;
  l_excelfile         blob;
  v_rc                sys_refcursor;

  l_monitor_record    monitors%rowtype;
  l_condition_result  number;
  l_next_exec         date;
  l_currentpage       pls_integer := 0;

begin
  begin
  select *
    into l_monitor_record
    from monitors
    where monitor_id = p_monitor_id;
  exception
    when no_data_found then
      raise_application_error(-20001,'Monitor ID ' || p_monitor_id || ' not found');
  end;

  if (l_monitor_record.EXPIRATION_EXPR is null and not p_force) then
    -- do nothing, we need an expiration expr
    return;
  else
    if (coalesce(l_monitor_record.NEXT_EXECUTION,sysdate) > sysdate and not p_force) then
      -- do nothing, execution not ready
      return;

    end if;
  end if;

  -- Evaluate if this monitor should be triggered
  l_condition_result := sql_condition_check(l_monitor_record.counter_sql,l_monitor_record.trigger_expr);
  insert into MONITOR_EXECUTION_HISTORY
    ( MONITOR_EXECUTION_HISTORY_ID,
      MONITOR_ID,
      TRIGGER_DATE,
      TRIGGER_EXPR_RESULT
    )
  values
    ( SEQ_MONITOR_EXECUTION_HISTORY.nextval,
      p_monitor_id,
      sysdate,
      l_condition_result
    );
  commit;

  execute immediate 'select ' || l_monitor_record.EXPIRATION_EXPR || ' from dual' into l_next_exec;
  update monitors
     set NEXT_EXECUTION  = l_next_exec
   where monitor_id      = p_monitor_id;
  commit;

  if (l_condition_result = 1 or p_force) then
    if (l_monitor_record.PLSQL_BLOCK is not null) then
      execute immediate l_monitor_record.PLSQL_BLOCK;
    end if;

    -- collect all recipients
    select distinct email
      bulk collect into l_receipts
      from recipients r
     inner join monitor_dls mdls on (mdls.distribution_list_id = r.distribution_list_id)
     where mdls.monitor_id = p_monitor_id;

      -- Ensure we have nothing hanging around from a previous run in this session
    as_xlsx.clear_workbook;

    for i in (select page_number,
                     page_title,
                     sql_report,
                     format
                from monitor_report_pages
               where monitor_id = p_monitor_id
               order by page_number asc
             ) loop
      as_xlsx.new_sheet(i.page_title);
      begin
        open v_rc for i.sql_report;
      exception
        when others then
          dbms_output.put_line('Failing SQL Statement');
          dbms_output.put_line(i.sql_report);
          raise;
      end;
      if (i.format is not null) then
        for l_colw in ( SELECT value,
                               rownum as coln
                          FROM json_table( i.format , '$.col_width[*]'
                                           COLUMNS (value PATH '$')
                                         )
                      ) loop
          as_xlsx.set_column_width
            ( p_col   => l_colw.coln,
              p_width => l_colw.value,
              p_sheet => i.page_number
            );
        end loop;
      end if;

      as_xlsx.query2sheet
        ( p_rc     => v_rc,
          p_sheet  => i.page_number,
          p_format => i.format
        );
      l_currentpage := i.PAGE_NUMBER;
    end loop;

    for i in ( select ds.DATA_SET_NAME,
                      sql_page_feeder,
                      format
                 from monitor_report_multipages mrmp
                inner join monitor_ht_data_sets ds on (mrmp.DS_PAGE_GENERATOR = ds.MONITOR_DATA_SET_ID)
                where mrmp.monitor_id = p_monitor_id
             ) loop
      for j in ( select key, value
                   from PKG_NOTIFICATION_SYSTEM.ht(i.DATA_SET_NAME)
               ) loop
        open v_rc for i.sql_page_feeder using j.key;
        l_currentpage := l_currentpage + 1;
        as_xlsx.new_sheet(j.key);
        as_xlsx.query2sheet
          ( p_rc     => v_rc,
            p_sheet  => l_currentpage,
            p_format => i.format
          );
      end loop;

    end loop;
    l_excelfile := as_xlsx.finish;

    send_email
      ( p_rcpt_list          => l_receipts,
        p_subject            => l_monitor_record.monitor_description ,
        p_message            => l_monitor_record.monitor_message,
        p_attachment_name    => l_monitor_record.file_name,
        p_attachment_mime    => 'application/vnd.ms-excel',
        p_attachment_content => l_excelfile
      );
  else
    dbms_output.put_line('Evaluates to false');
  end if;
end eval_monitor;

function ht( p_domain IN varchar2)
  return ht_table pipelined AS
  l_ht_row ht_row;
begin

  declare
    l_monitor_dataset_id monitor_ht_data_sets.monitor_data_set_id%type;
  begin
    select monitor_data_set_id
      into l_monitor_dataset_id
      from monitor_ht_data_sets
     where data_set_name = p_domain
       and sysdate > (last_refresh + EXPIRATION_MINS/60/24 );

    compute_ht_dataset(l_monitor_dataset_id);
  exception
    when no_data_found then
      null;
  end;

  FOR c_r IN ( SELECT key, value
                 FROM notification_system_ht
                WHERE ht_domain = p_domain
             ) LOOP
    l_ht_row.key := c_r.key;
    l_ht_row.value := c_r.value;
    PIPE ROW (l_ht_row);
  end loop;

  RETURN;
end ht;


function ht( p_cursor IN OUT NOCOPY number)
  return ht_table pipelined AS
  l_ht_row ht_row;
begin

  /*declare -- TO BE IMPLEMENTED
    l_monitor_dataset_id monitor_ht_data_sets.monitor_data_set_id%type;
  begin
    select monitor_data_set_id
      into l_monitor_dataset_id
      from monitor_ht_data_sets
     where data_set_name = p_domain
       and sysdate > (last_refresh + EXPIRATION_MINS/60/24 );

    compute_ht_dataset(l_monitor_dataset_id);
  exception
    when no_data_found then
      null;
  end; */
  if ( not dbms_sql.is_open(p_cursor)) then
    raise_application_error(-20001,'Parameter must be an opened cursor');
  end if;


  while (dbms_sql.FETCH_ROWS(p_cursor) > 0) loop
    null; --????
  end loop;
  /*FOR c_r IN ( SELECT key, value
                 FROM notification_system_ht
                WHERE ht_domain like p_domain || '%'
             ) LOOP
    l_ht_row.key := c_r.key;
    l_ht_row.value := c_r.value;
    PIPE ROW (l_ht_row);
  end loop;

  dbms_sql.close(p_cursor); */

  RETURN;
end ht;

procedure ht_add
  ( p_domain IN varchar2,
    p_key    IN varchar2,
    p_value  IN varchar2 DEFAULT NULL
  ) AS
begin
  merge into notification_system_ht t
  using ( select p_domain as "DOMAIN",
                 p_key as "KEY",
                 p_value as "VALUE"
            from dual
        ) s
    on (s."DOMAIN" = t."HT_DOMAIN" and s."KEY" = t."KEY")
  when matched then
    update set t.value = s.value
  when not matched then
    insert
      ( ht_domain,
        key,
        value
      )
    values
      ( s.domain,
        s.key,
        s.value
      );
  commit;
end ht_add;

function parametrize_string
  ( p_input_string IN varchar2,
    p_parameters   IN varchar2
  ) return varchar2 as
  l_retval varchar2(4000) := p_input_string;
  l_pivot  pls_integer;

begin
  FOR i IN ( SELECT value from json_table ( p_parameters, '$[*]' columns value path '$' )) LOOP
    l_pivot := instr(i.value,':');
    l_retval := replace(l_retval,'{' || substr(i.value,1,l_pivot-1) || '}', substr(i.value,l_pivot+1));
  end loop;
  return l_retval;
end parametrize_string;

procedure flush_ht_dataset
  ( p_monitor_data_set_id  monitor_ht_data_sets.monitor_data_set_id%type
  ) as
  l_dataset_name monitor_ht_data_sets.data_set_name%type;
begin
  select data_set_name
    into l_dataset_name
    from monitor_ht_data_sets
   where monitor_data_set_id = p_monitor_data_set_id;

  flush_ht(l_dataset_name);

end flush_ht_dataset;

procedure flush_ht
  ( p_domain IN varchar2
  ) as
begin
  delete from NOTIFICATION_SYSTEM_HT
   where HT_DOMAIN = p_domain;
  commit;
end flush_ht;

procedure flush_ht_suffix
  ( p_suffix IN varchar2
  ) as
begin
  delete from NOTIFICATION_SYSTEM_HT
   where HT_DOMAIN like p_suffix || '%';
  commit;
end flush_ht_suffix;

function display_dataset_prefix
  ( p_prefix IN varchar2 )
  return ht_table pipelined as
  l_ht_row ht_row;
begin
  FOR c_r IN ( SELECT HT_DOMAIN as key,
                      count(0) as value
                 FROM notification_system_ht
                WHERE ht_domain like p_prefix || '%'
                group by HT_DOMAIN
             ) LOOP
    l_ht_row.key := c_r.key;
    l_ht_row.value := c_r.value;
    PIPE ROW (l_ht_row);
  end loop;

  return;
end display_dataset_prefix;

procedure create_ht_dataset
  ( p_monitor_id       IN number,
    p_data_set_name    IN varchar2,
    p_sqlstr           IN clob,
    p_header           IN varchar2  default 'Y',
    p_cached           IN varchar2  default 'N',
    p_expiration_mins  IN number    default null,
    p_skip_if_exists   IN varchar2  default 'Y'
  ) as
  l_monitor_dataset_id number;
  l_cursor_check number;

begin
  l_cursor_check := dbms_sql.OPEN_CURSOR();
  dbms_sql.parse(l_cursor_check,q'[
select count(0)
  from monitor_ht_data_sets
 where monitor_id     = :monitor_id
   and data_set_name  = :data_set_name
    ]', dbms_sql.NATIVE );
  dbms_sql.bind_variable(l_cursor_check,':monitor_id', p_monitor_id);
  dbms_sql.bind_variable(l_cursor_check,':data_set_name',p_data_set_name);

  if   p_skip_if_exists = 'Y'
   and sql_condition_check
         ( p_cursor    => l_cursor_check,
           p_condition => ':1 = 1'
         ) = 1
      then
    return;
  end if;
  insert into monitor_ht_data_sets
    ( monitor_data_set_id,
      monitor_id,
      data_set_name,
      header,
      cached,
      expiration_mins,
      sqlstr
    )
  values
    ( seq_monitorhtdatasets.nextval,
      p_monitor_id,
      p_data_set_name,
      p_header,
      p_cached,
      p_expiration_mins,
      p_sqlstr
    ) returning monitor_data_set_id into l_monitor_dataset_id;
  commit;
  begin
    compute_ht_dataset(l_monitor_dataset_id);
  exception
    when others then
      if (dbms_sql.is_open(l_cursor_check)) then
        dbms_sql.CLOSE_CURSOR(l_cursor_check);
      end if;
      delete from monitor_ht_data_sets
       where MONITOR_DATA_SET_ID = l_monitor_dataset_id;
      commit;

      raise;
  end;
end create_ht_dataset;

function get_ds_id ( p_ds_name IN varchar2)
  return number as
  l_ds_id monitor_ht_data_sets.monitor_data_set_id%type;
begin
  select monitor_data_set_id
    into l_ds_id
    from monitor_ht_data_sets
   where data_set_name = p_ds_name;

  return l_ds_id;
end get_ds_id;

procedure compute_ht_dataset
  ( p_monitor_data_set_id in number
  ) as

  l_cursor   number;
  l_nbr_val  number;
  l_sqlstr   clob;

  l_dataset_name        monitor_ht_data_sets.data_set_name%type;
  l_key                 NOTIFICATION_SYSTEM_HT.key%type;
  l_value               NOTIFICATION_SYSTEM_HT.value%type;

begin
  select sqlstr,
         data_set_name
    into l_sqlstr,
         l_dataset_name
    from monitor_ht_data_sets
   where monitor_data_set_id = p_monitor_data_set_id;

  l_cursor := dbms_sql.open_cursor;
  dbms_sql.parse(l_cursor,l_sqlstr,dbms_sql.native);
  dbms_sql.define_column(l_cursor,1, l_key, getsize(l_key));
  dbms_sql.define_column(l_cursor,2,l_value, getsize(l_value));

  l_nbr_val := dbms_sql.execute(l_cursor);
  flush_ht_dataset(p_monitor_data_set_id);

  while (dbms_sql.FETCH_ROWS(l_cursor )> 0) loop
    dbms_sql.column_value(l_cursor, 1, l_key);
    dbms_sql.column_value(l_cursor, 2, l_value);
    ht_add
      ( p_domain  => l_dataset_name,
        p_key     => l_key,
        p_value   => l_value
      );
  end loop;

  dbms_sql.CLOSE_CURSOR(l_cursor);

  update monitor_ht_data_sets
     set last_refresh = sysdate
   where MONITOR_DATA_SET_ID = p_monitor_data_set_id;
  commit;

exception
  when others then
    if dbms_sql.IS_OPEN(l_cursor) then
      dbms_sql.close_cursor(l_cursor);
    end if;
    raise;
end;

function foreign_getddl
  ( p_owner       IN varchar2,
    p_object_type IN varchar2,
    p_name        IN varchar2
  ) return clob as
  l_fqn_function_name varchar2(70);
  invalid_identifier exception;

  l_sqlstr varchar2(4000) := q'[

    ]';

  pragma exception_init(invalid_identifier,-44002);
begin
  l_fqn_function_name := '"' || p_owner || '"."GETDDL"';
  --dbms_assert.SQL_OBJECT_NAME(l_fqn_function_name);
  return null;
exception
  when invalid_identifier then
    raise_application_error(-20001,'GETDDL function does not exists on ' || p_owner);
    raise;
end;

procedure run as

begin
  for c_r in (select monitor_id from monitors) loop
    declare
      l_errm MONITOR_EXECUTION_HISTORY.error_message%type;
    begin
      eval_monitor(p_monitor_id => c_r.MONITOR_ID);
    exception
      when others then
        -- Report errors if any and continue
        dbms_output.put_line('Failed to evaluate monitor # ' || c_r.monitor_id);
        dbms_output.put_line(sqlerrm);
        l_errm := sqlerrm || utl_tcp.crlf;
        dbms_output.put_line(dbms_utility.format_error_backtrace);
        l_errm := l_errm || dbms_utility.format_error_backtrace;
        insert into MONITOR_EXECUTION_HISTORY
          ( MONITOR_EXECUTION_HISTORY_ID,
            MONITOR_ID,
            TRIGGER_DATE,
            ERROR_MESSAGE
          )
        values
          ( SEQ_MONITOR_EXECUTION_HISTORY.nextval,
            c_r.monitor_id,
            sysdate,
            l_errm
          );
        commit;
    end;
  end loop;
end run;

procedure add_recipient
  ( p_monitor_id IN monitors.monitor_id%type,
    p_recipient  IN varchar2
  ) as
  l_dl_id  DISTRIBUTION_LISTS.distribution_list_id%type;
begin
  begin
    select distribution_list_id
      into l_dl_id
      from MONITOR_DLS
     where monitor_id = p_monitor_id
     fetch first 1 row only;
  exception
    when no_data_found then
      insert into DISTRIBUTION_LISTS
        ( DISTRIBUTION_LIST_ID,
          DESCRIPTION
        )
      values
        ( SEQ_DISTRIBUTION_LISTS.nextval,
          'Default DL for monitorID ' || p_monitor_id
        )
      returning DISTRIBUTION_LIST_ID into l_dl_id;
      commit;

      insert into monitor_dls
        ( MONITOR_DLS_ID,
          MONITOR_ID,
          DISTRIBUTION_LIST_ID
        )
      values
        ( SEQ_MONITOR_DLS.nextval,
          p_monitor_id,
          l_dl_id
        );
      commit;
  end;

  insert into RECIPIENTS
    ( RECIPIENT_ID,
      DISTRIBUTION_LIST_ID,
      EMAIL
    )
  values
    ( SEQ_RECIPIENTS.nextval,
      l_dl_id,
      p_recipient
    );
  commit;

end add_recipient;

function reason
    ( p_left_op in number,
      p_right_op in number,
      p_operator in number,
      reason in varchar2) return dpx_NOTIFICATION_SYSTEM.r_reason_nbr as
begin
  return dpx_NOTIFICATION_SYSTEM.r_reason_nbr(p_left_op, p_right_op, p_operator, reason);
end;

begin
  select parameter_value
    into gl_setup_set
    from notification_system_parameters
   where parameter_name = 'Setup Set'
    and setup_set = '*';
exception
  when no_data_found then
    raise_application_error(-20001,'Run notification system setup first');
end dps_notification_system;
/