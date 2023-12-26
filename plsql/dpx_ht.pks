create or replace package dpx_ht AS

    TYPE ht_row IS RECORD
        ( id     hashtables.id%type,
          value  hashtables.value%type
        );

    TYPE ht_table IS TABLE OF ht_row;

    function display
        return ht_table pipelined;

    procedure add
        ( p_key    IN varchar2,
          p_value  IN varchar2
        );
end dpx_ht;
/