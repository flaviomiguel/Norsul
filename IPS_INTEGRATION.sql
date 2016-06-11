CREATE OR REPLACE PACKAGE IPS_INTEGRATION AS 

 PROCEDURE load_postaging_table;
 PROCEDURE load_rejectstaging_table;
 PROCEDURE load_cancelling_table;
 PROCEDURE load_mrstatus_table;
 PROCEDURE proc_create_po;
 PROCEDURE proc_reject_mr;
 PROCEDURE proc_cancelling_po;
 PROCEDURE proc_upd_mrstatus;

END IPS_INTEGRATION;
 
/


CREATE OR REPLACE PACKAGE BODY IPS_INTEGRATION
AS
  PROCEDURE PROC_CREATE_PO
  AS
    V_OBJECT_CODE STARIPS.ST_OBJECT.OBJ_CODE%TYPE;
    V_COUNT   NUMBER :=0;
    v_item_no NUMBER :=0;
    v_obj_id starips.st_object.object_id%type;
    vtab_object starips.st_object%rowtype;
    vtab_pohead starips.st_poahead%rowtype;
    vtab_poitem starips.st_poaitem%rowtype;
    v_load_seq sysint.sysint_int_ips_protheus.load_seq%TYPE := '00000';
    v_errorlog_id NUMBER;
    v_updatemr boolean;
  BEGIN
    /*Atualizando PO Header */
    FOR j IN
    (SELECT  POA.*,
      PROT.LOAD_SEQ,
      PROT.NATUREZA,
      PROT.moeda,
      PROT.mr_object_id,
      PROT.fornecedor,
      PROT.nome_fornecedor,
      PROT.po_number,
      PROT.data_entrega,
      PROT.usuario,
      PROT.data_emissao,
      PROT.prc_tot
    FROM sysint_poahead_basic poa
    JOIN (SELECT DISTINCT P.LOAD_SEQ,
                 P.NATUREZA,
                 p.moeda,
                 p.mr_object_id,
                p.fornecedor,
                p.nome_fornecedor,
                p.po_number,
                p.data_entrega,
                p.usuario,
                p.data_emissao,
                p.prc_tot,
                p.po_object_id,
                p.imp_error_id
                FROM sysint_int_ips_protheus p
                WHERE ROWID = (SELECT ROWID FROM sysint_int_ips_protheus SIP WHERE LOAD_SEQ = P.LOAD_SEQ AND ROWNUM =1)) PROT
    ON poa.po_object_id = PROT.mr_object_id
    WHERE prot.po_object_id IS NULL
    and prot.imp_error_id is null
    )
    LOOP
      BEGIN
        IF v_load_seq <> j.load_seq THEN
          /*Recebe novo codigo PO*/
          v_object_code := ips_tools.get_new_pocode(j.po_number,ips_tools.get_obj_class(j.mr_object_id));
          v_obj_id      := ips_tools.get_new_objid;
          V_ITEM_NO     :=0;
          /*Criando object*/
          vtab_object.object_id       := v_obj_id;
          vtab_object.objclass_id     := ips_tools.get_obj_class(j.mr_object_id);
          vtab_object.objtype_id      := 520;
          vtab_object.obj_code        := v_object_code;
          vtab_object.obj_description := ips_tools.get_obj_description(j.mr_object_id);
          vtab_object.rec_deleted     := 0;
          INSERT INTO starips.st_object VALUES vtab_object;
        END IF;
        v_load_seq := j.load_seq;
        
        vtab_pohead.po_object_id     := v_obj_id; 
        vtab_pohead.postatus_id      := ips_tools.GET_STATUS_ID ('Ordered');
        vtab_pohead.po_statusdate    := SYSDATE;
        vtab_pohead.popriority       := j.popriority;
        vtab_pohead.poclass_id       := j.poclass_id;
        vtab_pohead.account_id       := j.natureza;
        vtab_pohead.ADDR_OBJECT_ID   := ips_tools.get_addr_id(J.fornecedor,j.nome_fornecedor);
        vtab_pohead.po_attention     := j.po_attention;
        vtab_pohead.curr_id          := NVL(J.MOEDA,j.curr_id);
        vtab_pohead.po_curr_rate     := j.po_curr_rate;
        vtab_pohead.po_ipsorder      := j.po_ipsorder;
        vtab_pohead.po_mrid          := ips_tools.get_obj_code(j.mr_object_id);
        vtab_pohead.PO_MR_OBJECT_ID  := TRIM(j.MR_OBJECT_ID);
        vtab_pohead.depart_id        := j.depart_id;
        vtab_pohead.po_nextduedate   := j.po_nextduedate;
        vtab_pohead.PO_MRCREATEDBY   := J.PO_MRCREATEDBY;
        vtab_pohead.po_mrcreateddate := j.po_mrcreateddate;
        vtab_pohead.PO_ORDEREDBY     := j.usuario;
        vtab_pohead.PO_ORDEREDDATE   := J.DATA_EMISSAO;
        vtab_pohead.po_itemtotal     := j.prc_tot;
        vtab_pohead.po_payterms      := j.po_payterms;
        vtab_pohead.podel_code       := j.podel_code;
        vtab_pohead.PO_DELTERMS      := J.PO_DELTERMS;
        vtab_pohead.rec_credate      := j.rec_credate;
        vtab_pohead.REC_CREATOR      := J.REC_CREATOR;
        vtab_pohead.po_instruction   := j.po_instruction;
        vtab_pohead.po_cancelledby   := j.po_cancelledby;
        vtab_pohead.po_cancelleddate := j.po_cancelleddate;
        vtab_pohead.po_deliverydate  := j.data_entrega;
        vtab_pohead.po_neededonplant := j.po_neededonplant;
        VTAB_POHEAD.REC_dELETED      :=0;
        INSERT
        INTO STARIPS.ST_POAHEAD VALUES vtab_pohead;
        /*INSERT LINE ITEMS*/
        FOR I IN
        (SELECT * FROM sysint_int_ips_protheus WHERE load_seq = j.load_seq
        )
        LOOP
          V_ITEM_NO := V_ITEM_NO +1;
          BEGIN
            vtab_poitem.po_object_id     := v_obj_id;
            vtab_poitem.poitem_id        := v_item_no;--trim(i.item_id);
            vtab_poitem.matunit_id       := trim(i.um);
            vtab_poitem.poitem_no        := trim(i.item_id); --v_item_no;
            vtab_poitem.mat_object_id    := IPS_TOOLS.GET_ITEM_ID(trim(i.produto)); -- ips_tools.get_item_id : CRIAR FUNCAR PARA VERIFICAR SE O ITEM EXISTE I.MAT_OBJECT_ID,
            vtab_poitem.poitem_code      := trim(i.produto);
            vtab_poitem.poitem_desc      := nvl(ips_tools.get_obj_description(IPS_TOOLS.GET_ITEM_ID(trim(i.produto))),trim(i.descricao)); --trim(i.descricao);
            vtab_poitem.poitem_unitprice := trim(i.prc_unit);
            vtab_poitem.poitem_ordered   := i.quant;
            vtab_poitem.rec_deleted      := 0;
            INSERT INTO starips.st_poaitem VALUES vtab_poitem;
          END;
             -- Update MR to ordered
          v_updatemr := ips_tools.set_mrpo_status(j.mr_object_id,'Ordered');
        
        END LOOP;
        
             COMMIT;
        
        -- set PO T.A
        ips_tools.set_obj_ta (
          p_object_id  => v_obj_id,
          p_plant_id   => ips_tools.get_plant(j.mr_object_id),
          p_funcclass_id  => ips_tools.get_obj_class(j.mr_object_id),
          p_function_id  => ips_tools.get_mr_function(j.mr_object_id),
          p_objtype_id  => 520);
        
        
        --- SYSINT_IMPORT_CONTROL
          UPDATE sysint_int_ips_protheus
          SET po_object_id = v_obj_id,
            ips_imp_date   = sysdate
          WHERE load_seq   = j.load_seq;
          COMMIT;
          
        
      EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK;
        ips_tools.ERROR_LOG(
        p_object_id     => NULL,
        P_CODE_ID       => J.PO_NUMBER,
        P_ERROR_DESCR   => SQLERRM,
        p_error_code    => sqlcode,
        p_error_message => 'FAILED TO CREATE PO',
        p_error_number => v_errorlog_id);
        
          UPDATE sysint_int_ips_protheus
          SET imp_error_id =v_errorlog_id
          WHERE load_seq   = j.load_seq;
        
        COMMIT;
      END;
    END LOOP;
  END;
  
  
PROCEDURE load_postaging_table
AS
  vtab_staging sysint.sysint_int_ips_protheus%rowtype;
  vtab_seq NUMBER;
  vtab_po  VARCHAR2(30) := '1111';
  v_errorlog_id number;
 
BEGIN
  FOR i  IN
  (SELECT * FROM SYSINT_LOAD_POSTATINGTAB_V
        ORDER BY PO_NUMBER || object_id ,object_id,ITEM_ID)
  LOOP
    IF vtab_po  <> i.po_number ||trim(i.object_id) THEN
      vtab_seq := ips_tools.get_stagingtab_seq;
    END IF;
    vtab_po                      := trim(i.po_number) || trim(i.object_id);
    --------------------------------------------------
    vtab_staging.po_number       := I.PO_NUMBER;
    vtab_staging.load_seq        := vtab_seq;
    vtab_staging.load_date       := sysdate;
    vtab_staging.tipo            := trim(i.tipo);
    vtab_staging.mr_number       := trim(i.po_id);
    vtab_staging.mr_object_id    := to_number(trim(i.object_id));
    vtab_staging.item_id         := to_number(trim(i.item_id));
    vtab_staging.filial          := trim(i.filial);
    vtab_staging.num             := trim(i.num);
    vtab_staging.item            := trim(i.item);
    vtab_staging.fornecedor      := trim(i.fornecedor);
    vtab_staging.loja            := trim(i.loja);
    vtab_staging.nome_fornecedor := trim(i.nome_fornecedor);
    vtab_staging.produto         := trim(i.produto);
    vtab_staging.descricao       := trim(i.descricao);
    vtab_staging.um              := trim(i.um);
    vtab_staging.quant           := trim(i.quant);
    vtab_staging.quant_entregue  := trim(i.quant_entregue);
    vtab_staging.prc_unit        := trim(i.prc_unit);
    vtab_staging.prc_tot         := trim(i.prc_tot);
    vtab_staging.moeda           := trim(I.moeda);
    vtab_staging.taxa_moeda      := trim(i.taxa_moeda);
    vtab_staging.data_emissao    := to_date(trim(i.data_emissao),'YYYYMMDD');
    vtab_staging.data_entrega    := to_date(trim(i.data_entrega),'YYYYMMDD');
    vtab_staging.natureza        := trim(i.natureza);
    vtab_staging.deletado        := trim(i.deletado);
    vtab_staging.nome_comprador  := TRIM(I.NOME_COMPRADOR);
    vtab_staging.num_req         := trim(i.num_req);
    vtab_staging.tipo_produto    := trim(i.tipo);
    vtab_staging.usuario         := trim(i.usuario);
    BEGIN
      INSERT INTO sysint_int_ips_protheus VALUES vtab_staging;
      COMMIT;
    EXCEPTION
    WHEN OTHERS THEN
      ips_tools.error_log( p_object_id => NULL, 
      p_code_id => i.po_number, 
      p_error_descr => sqlerrm, 
      p_error_code => sqlcode, 
      p_error_message => 'FALHA AO IMPORTAR A PO: '||i.po_number,
      p_error_number => v_errorlog_id);
      ROLLBACK;
    END;
  END LOOP;
END; 
  
PROCEDURE load_rejectstaging_table
AS
  vtab_staging sysint.sysint_int_ips_protheus_reject%rowtype;
  vtab_seq NUMBER;
  vtab_mrid  VARCHAR2(20) := '1111';
  v_errorlog_id number;
BEGIN
  FOR i  IN
  (SELECT * FROM sysint_load_rejectstatingtab_v
        ORDER BY OBJECT_ID)
  loop
    IF vtab_mrid <> i.object_id THEN
      vtab_seq := ips_tools.get_stagingtab_seq;
    END IF;
    vtab_mrid                      := i.object_id;
    --------------------------------------------------
    vtab_staging.object_id       := vtab_mrid;
    vtab_staging.load_seq        := vtab_seq;
    vtab_staging.load_date       := SYSDATE;
    vtab_staging.dt_canc         := nvl(to_date(trim(i.dt_canc),'yyyymmdd'),sysdate);
    vtab_staging.comentario      := trim(i.comentario);
    vtab_staging.nome_user       := TRIM(i.nome_user);
 
    BEGIN
      INSERT INTO sysint_int_ips_protheus_reject VALUES vtab_staging;
      COMMIT;
    EXCEPTION
    WHEN others THEN
      ROLLBACK;
      ips_tools.error_log( p_object_id => i.OBJECT_ID, 
      p_code_id => i.object_id, 
      p_error_descr => sqlerrm, 
      p_error_code => sqlcode, 
      p_error_message => 'FALHA AO IMPORTAR A MR REJEITADA PROTHEUS -> SYSINT: OBJ_ID: '||i.OBJECT_ID,
      p_error_number => v_errorlog_id);
      
      COMMIT;
    END;
  END loop;
END; 
  
PROCEDURE load_cancelling_table
AS
  vtab_staging sysint.sysint_int_ips_protheus_cancel%rowtype;
  vtab_seq NUMBER;
  v_errorlog_id number;
BEGIN
  FOR i  IN
  (SELECT * FROM sysint_load_cancelling_v
        ORDER BY po_number)
  loop
     vtab_seq := ips_tools.get_stagingtab_seq;
    --------------------------------------------------
    vtab_staging.po_number       := i.po_number;
    vtab_staging.mr_object_id    := i.object_id;
    vtab_staging.DELETADO        := I.DELETADO;
    vtab_staging.load_seq        := vtab_seq;
    vtab_staging.load_date       := SYSDATE;

 
    BEGIN
      INSERT INTO sysint_int_ips_protheus_cancel VALUES vtab_staging;
      COMMIT;
    EXCEPTION
    WHEN others THEN
      ROLLBACK;
      ips_tools.error_log( p_object_id => i.OBJECT_ID, 
      p_code_id => i.object_id, 
      p_error_descr => sqlerrm, 
      p_error_code => sqlcode, 
      p_error_message => 'FALHA AO IMPORTAR A PO CANCELADA PROTHEUS -> SYSINT: OBJ_ID: '||i.OBJECT_ID,
      p_error_number => v_errorlog_id);
      
      COMMIT;
    END;
  END loop;
END;   
  
PROCEDURE load_mrstatus_table
AS
  vtab_staging sysint.sysint_int_ips_protheus_mrupd%rowtype;
  vtab_seq NUMBER;
  v_errorlog_id number;
BEGIN
  FOR i  IN
  (SELECT * FROM sysint_load_mrstatus_v
        ORDER BY object_id)
  loop
     vtab_seq := ips_tools.get_stagingtab_seq;
    --------------------------------------------------
    vtab_staging.tipo            := i.tipo;
    vtab_staging.mr_object_id    := i.object_id;
    vtab_staging.load_seq        := vtab_seq;
    vtab_staging.load_date       := SYSDATE;

 
    BEGIN
      INSERT INTO sysint_int_ips_protheus_mrupd VALUES vtab_staging;
      COMMIT;
    EXCEPTION
    WHEN others THEN
      ROLLBACK;
      ips_tools.error_log( p_object_id => i.OBJECT_ID, 
      p_code_id => i.object_id, 
      p_error_descr => sqlerrm, 
      p_error_code => sqlcode, 
      p_error_message => 'FALHA AO IMPORTAR MR STATUS DO PROTHEUS -> SYSINT: OBJ_ID: '||i.OBJECT_ID,
      p_error_number => v_errorlog_id);
      
      COMMIT;
    END;
  END loop;
END;   
  
PROCEDURE PROC_REJECT_MR
AS
  v_true        BOOLEAN;
  v_errorlog_id NUMBER;
BEGIN
  FOR i IN
  (SELECT * FROM sysint_int_ips_protheus_reject WHERE ips_imp_date IS NULL and imp_error_id is null order by load_Seq
  )
  LOOP
    BEGIN
      v_true := ips_tools.SET_MRPO_STATUS(p_object_id => i.object_id, p_status => 'Cancelled');
      ips_tools.add_comment( p_object_id => i.object_id, p_comment => i.comentario, p_name => i.nome_user, p_date => sysdate);
      
      UPDATE sysint_int_ips_protheus_reject
       set ips_imp_date =sysdate
      WHERE load_seq   = I.load_seq;
      COMMIT;
      
      
    EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ips_tools.ERROR_LOG( p_object_id => NULL, P_CODE_ID => i.object_id, P_ERROR_DESCR => SQLERRM, p_error_code => SQLCODE, p_error_message => 'FALHA AO IMPORTAR A MR REJEITADA SYSINT -> IPS: OBJ_ID: '|| I.OBJECT_ID, p_error_number => v_errorlog_id);
      UPDATE sysint_int_ips_protheus_reject
      SET imp_error_id =v_errorlog_id
      WHERE load_seq   = I.load_seq;
      COMMIT;
    END;
  END LOOP;
END; 
   
  PROCEDURE PROC_CANCELLING_PO
  AS 
   v_errorlog_id NUMBER;
   BEGIN
     FOR i IN (SELECT * FROM sysint_int_ips_protheus_cancel WHERE IPS_IMP_date IS NULL and IMP_ERROR_ID IS NULL order by load_seq)
      loop
        BEGIN
          UPDATE starips.st_poahead
            SET postatus_id = ips_tools.get_status_id('Cancelled'),
                po_cancelleddate = SYSDATE,
                po_cancelledby = 'GESUP'
          WHERE po_object_id IN (SELECT object_id FROM starips.st_object WHERE obj_code LIKE i.po_number ||'%');
          
           UPDATE sysint_int_ips_protheus_cancel
             SET ips_imp_date = SYSDATE
           WHERE LOAD_SEQ = I.LOAD_SEQ;
         
         COMMIT;
        
       EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          ips_tools.ERROR_LOG( p_object_id => NULL, P_CODE_ID => i.PO_NUMBER, P_ERROR_DESCR => SQLERRM, p_error_code => SQLCODE, p_error_message => 'FALHA AO CANCELAR A PO  SYSINT -> IPS: OBJ_ID: '|| I.PO_NUMBER, p_error_number => v_errorlog_id);
          UPDATE sysint_int_ips_protheus_cancel
          SET imp_error_id =v_errorlog_id
          WHERE load_seq   = I.load_seq;
          COMMIT;
        END;
      END LOOP;
   END;
  
PROCEDURE PROC_UPD_MRSTATUS
AS
  v_errorlog_id NUMBER;
  v_result      BOOLEAN;
  V_STATUS      VARCHAR2(20);
BEGIN
  FOR i  IN
  (SELECT *
  FROM sysint_int_ips_protheus_MRUPD
  WHERE IPS_IMP_date IS NULL
  AND imp_error_id   IS NULL
  order by load_Seq
  )
  LOOP
    BEGIN
      SELECT DECODE(i.tipo,1,'Tender','Ready at suppl.') INTO v_status FROM dual;
      V_RESULT := IPS_TOOLS.SET_MRPO_STATUS(I.MR_OBJECT_ID,V_STATUS);
      UPDATE sysint_int_ips_protheus_MRUPD
      SET ips_imp_date = SYSDATE
      WHERE LOAD_SEQ   = I.LOAD_SEQ;
      COMMIT;
    EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ips_tools.error_log( p_object_id => NULL, p_code_id => i.mr_object_id, p_error_descr => sqlerrm, p_error_code => sqlcode, p_error_message => 'FALHA AO ATUALIZAR O STATUS DA MR SYSINT -> IPS: OBJ_ID: '|| i.mr_object_id, p_error_number => v_errorlog_id);
      UPDATE sysint_int_ips_protheus_mrupd
      SET imp_error_id =v_errorlog_id
      WHERE load_seq   = I.load_seq;
      COMMIT;
    END;
  END LOOP;
END;
  
  
END IPS_INTEGRATION;
/
