CREATE OR REPLACE PROCEDURE PA.PA_REGISTRA_IMPRESION(P_SISTEMA  in varchar2,
                               P_NOM_FORMA  in varchar2,
                               P_AGENCIA    in varchar2,
                               P_CLIENTE  in varchar2,
                               P_OPERACION  in varchar2,
                               P_NOM_DOCUMENTO  in varchar2,
                               P_NUM_PAGINAS in number,
                               P_IMPRESORA  in varchar2,
                               P_DESCRIPCION  in varchar2,
                               P_ERROR  OUT varchar2
                               )  IS
vpsUserBD varchar2(30);
vpsOsUser varchar2(14);
vpsMaquina varchar2(100);
vpsTerminal varchar2(50);
vpsProgram varchar2(48);
vpsModule varchar2(48);
vpdLogonTime date;

BEGIN
/*------------------------------------------------------*/
--HEALVAREZ-05/06/2017-Inclusion de generacion de logs de impresiÃ³n.
--Procedimiento que se debe incluir en toda impresion realizada desde unibanca.
--P_NOM_FORMA     -> Nombre de la forma donde se realiza la impresion.
--P_AGENCIA       -> Codigo de la agencia de conexion.
--P_CLIENTE       -> Codigo de cliente de la operacion relacionada.
--P_OPERACION     -> Codigo de operacion relacionada. (Por ejem.: Numero de cuenta de efectivo, numero de tramite,.....)
--P_NOM_DOCUMENTO -> Nombre del documento o reporte impreso.
--P_NUM_PAGINAS   -> Numero de paginas de la impresion realizada.
--P_IMPRESORA     -> Nombre la de impresora por defecto de la maquina o servidor donde se realiza la impresion.
--P_DESCRIPCION   -> Descripcion general de la impresion.
--P_ERROR         -> Valor de retorno, si ocurrio algun error.
/*------------------------------------------------------*/
    P_ERROR:='';

    begin
        Select USER,osuser,Replace(MACHINE,CHR(0),''), terminal, program, MODULE, LOGON_TIME
            into vpsUserBD,vpsOsUser,vpsMaquina,vpsTerminal,vpsProgram,vpsModule,vpdLogonTime
        From V$SESSION
        Where USERNAME = USER
          And AUDSID = USERENV('sessionid')
          And STATUS = 'ACTIVE' AND ROWNUM=1;
    exception
        when others then
            vpsUserBD   := '0';
            vpsOsUser   := '0';
            vpsMaquina  := '0';
            vpsTerminal := '0';
            vpsProgram  := '0';
            vpsModule   := '0';
            vpdLogonTime:= null;
    end;

    Insert into PA.PA_IMPRESIONES_LOG
       (SISTEMA, NOM_FORMA, COD_AGENCIA, COD_CLIENTE, OPERACION, NOMBRE_DOCUMENTO, NUMERO_PAGINAS, IMPRESORA_DEFECTO, 
        DESCRIPCION, FECHA, BD_USER, OS_USER, 
        MACHINE, TERMINAL_US, PROGRAMA, MODULE, LOGON_TIME)
     Values
       (substr(P_SISTEMA,0,10), SUBSTR(P_NOM_FORMA,0,20), substr(P_AGENCIA,0,10), substr(P_CLIENTE,0,20), substr(P_OPERACION,0,100), substr(P_NOM_DOCUMENTO,0,100), P_NUM_PAGINAS, substr(P_IMPRESORA,0,200), 
        substr(P_DESCRIPCION,0,1000), SYSDATE, vpsUserBD, vpsOsUser, 
        vpsMaquina, vpsTerminal, vpsProgram, vpsModule, vpdLogonTime);

    commit;      

EXCEPTION
    WHEN OTHERS THEN
        rollback;
        P_ERROR    :=    'Ocurrio un error en el registro del log de impresion - '||' - '||SQLERRM||'-'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        dbms_output.put_line(P_ERROR);
END;
/