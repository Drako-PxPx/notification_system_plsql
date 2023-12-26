create or replace package body dpx_ht AS

    function display
        return ht_table pipelined as
        l_ht_row ht_row;
    begin
        for c_r IN ( SELECT id,
                            value
                       FROM hashtables
                   ) LOOP
            l_ht_row.id := c_r.id;
            l_ht_row.value := c_r.value;
            PIPE ROW (l_ht_row);
        end loop;
    end display;

    procedure add
        ( p_key    IN varchar2,
          p_value  IN varchar2
        ) as
    begin
        merge into hashtables ht
        using ( select ora_hash(p_key)  as id,
                       p_value          as val
                  from dual
              ) s
        on ( s.id = ht.id )
        when matched then
          update set ht.value = s.val
        when not matched then
          insert
              ( id,
                value
              )
          values
              ( s.id,
                s.val
              );
    end add;
end dpx_ht;
/