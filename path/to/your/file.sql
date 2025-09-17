CREATE OR REPLACE PROCEDURE public.r_clean_mdm_product_optimized() AS $BODY$
/*
描述：处理MDM清洗的数据更新到业务表中
时间：2023-12-19
人员：Huazi

描述：增加商户自定义规则的处理
时间：2024-05-09

描述：优化版本，使用集合操作替代循环，提高性能
时间：2025-09-01
人员：AI助手
*/

BEGIN
    -- 确保所有操作作为一个事务，如果任何部分失败，则全部回滚
    -- PostgreSQL默认在存储过程调用时开始事务，除非有明确的COMMIT或ROLLBACK。
    -- 移除原代码中的COMMIT，保持整个过程的原子性。

    -- 临时表用于简化后续操作
    CREATE TEMP TABLE temp_clean_details ON COMMIT DROP AS
    SELECT
        t1.id AS job_id,
        t2.product_id,
        t2.suggest_oe,
        t2.suggest_brand,
        t2.suggest_quality,
        t2.brand,
        pt.company_id
    FROM
        b2b_clean_job AS t1
    JOIN
        b2b_clean_job_details AS t2 ON t1.id = t2.job_id
    JOIN
        product_template AS pt ON t2.product_id = pt.id
    WHERE
        t1.state = 'completed';

    -- 1. 批量处理自定义规则的品牌和品质更新
    UPDATE product_template AS pt
    SET
        part_brand_id = bm.part_brand_id,
        part_type_id = bm.part_type_id
    FROM
        temp_clean_details AS tcd
    JOIN
        b2b_product_mapping AS bm ON bm.mapping_type = 'Custom'
                                  AND bm.source_name = tcd.brand
                                  AND bm.company_id = tcd.company_id
                                  AND bm.state = '清洗成功'
    WHERE
        pt.id = tcd.product_id;

    -- 2. 批量处理非自定义规则的品牌和品质更新
    WITH standard_updates AS (
        SELECT
            tcd.product_id,
            pb.id AS new_part_brand_id,
            pt.id AS new_part_type_id
        FROM
            temp_clean_details AS tcd
        LEFT JOIN
            b2b_product_mapping AS bm ON bm.mapping_type = 'Custom'
                                      AND bm.source_name = tcd.brand
                                      AND bm.company_id = tcd.company_id
                                      AND bm.state = '清洗成功'
        JOIN
            part_brand AS pb ON pb.name = tcd.suggest_brand
        JOIN
            part_type AS pt ON pt.name = tcd.suggest_quality
        WHERE
            bm.source_name IS NULL
    )
    UPDATE product_template AS pt_update
    SET
        part_brand_id = su.new_part_brand_id,
        part_type_id = su.new_part_type_id
    FROM
        standard_updates AS su
    WHERE
        pt_update.id = su.product_id;

    -- 3. 批量插入或更新产品清洗规则（使用ON CONFLICT）
    WITH standard_mappings AS (
        SELECT DISTINCT ON (tcd.company_id, tcd.brand)
            tcd.company_id,
            tcd.brand,
            pb.id AS new_part_brand_id,
            pt.id AS new_part_type_id
        FROM
            temp_clean_details AS tcd
        LEFT JOIN
            b2b_product_mapping AS bm ON bm.mapping_type = 'Custom'
                                      AND bm.source_name = tcd.brand
                                      AND bm.company_id = tcd.company_id
                                      AND bm.state = '清洗成功'
        JOIN
            part_brand AS pb ON pb.name = tcd.suggest_brand
        JOIN
            part_type AS pt ON pt.name = tcd.suggest_quality
        WHERE
            bm.source_name IS NULL
    )
    INSERT INTO b2b_product_mapping (write_date, mapping_type, company_id, source_name, part_type_id, part_brand_id, state)
    SELECT
        now(),
        'System',
        sm.company_id,
        sm.brand,
        sm.new_part_type_id,
        sm.new_part_brand_id,
        '清洗成功'
    FROM
        standard_mappings AS sm
    ON CONFLICT (company_id, source_name) DO UPDATE SET
        part_type_id = EXCLUDED.part_type_id,
        part_brand_id = EXCLUDED.part_brand_id,
        state = EXCLUDED.state;

    -- 4. 批量处理OE数据
    CREATE TEMP TABLE temp_parsed_oe ON COMMIT DROP AS
    SELECT
        tcd.product_id,
        j.value->>'id' AS oe_uuid,
        j.value->>'oe' AS oe_number,
        j.value->>'name' AS oe_name,
        j.value->>'maker' AS maker_name
    FROM
        temp_clean_details AS tcd,
        jsonb_array_elements(tcd.suggest_oe::jsonb) AS j;

    -- 4.1 批量插入新的OE数据
    -- 修正: 增加一个 LEFT JOIN 来检查 oe_number 和 maker_id 组合是否已存在
    WITH new_oes AS (
        SELECT DISTINCT ON (tp.oe_number, tp.maker_name)
            tp.oe_uuid,
            tp.oe_number,
            tp.oe_name,
            tp.maker_name
        FROM
            temp_parsed_oe AS tp
        LEFT JOIN
            oe_number AS oe ON oe.uuid = tp.oe_uuid
        LEFT JOIN
            make AS mk ON mk.name = tp.maker_name
        LEFT JOIN
            oe_number AS existing_oe ON existing_oe.oe_number = tp.oe_number AND existing_oe.make_id = mk.id
        WHERE
            oe.id IS NULL AND existing_oe.id IS NULL
    )
    INSERT INTO oe_number (id, uuid, name, oe_number, create_uid, create_date, write_uid, write_date, oe_number_trim, make_id, is_verify, verify_results, oe_name)
    SELECT
        nextval('oe_number_id_seq'),
        n.oe_uuid,
        n.oe_name,
        n.oe_number,
        1, now(), 1, now(), n.oe_number, (SELECT id FROM make WHERE name = n.maker_name), 't', 't', n.oe_name
    FROM
        new_oes AS n;

    -- 4.2 批量更新现有OE的UUID
    UPDATE oe_number AS oe
    SET
        uuid = tp.oe_uuid,
        write_date = now()
    FROM
        temp_parsed_oe AS tp
    WHERE
        oe.uuid IS DISTINCT FROM tp.oe_uuid
        AND oe.oe_number = tp.oe_number
        AND oe.make_id = (SELECT id FROM make WHERE name = tp.maker_name);

    -- 4.3 批量插入/更新产品与OE的关系
    INSERT INTO part_to_oe1 (part_id, oe_id, write_date, type)
    SELECT DISTINCT ON (tp.product_id, oe.id)
        tp.product_id,
        oe.id,
        now(),
        'mdm_clean'
    FROM
        temp_parsed_oe AS tp
    JOIN
        oe_number AS oe ON oe.uuid = tp.oe_uuid
    ON CONFLICT (part_id, oe_id) DO UPDATE SET
        write_date = now(),
        type = 'mdm_clean';

    -- 5. 批量更新任务状态
    UPDATE b2b_clean_job
    SET
        state = 'finish',
        finish_date = now() + interval'8 hour'
    WHERE
        id IN (SELECT DISTINCT job_id FROM temp_clean_details);

    -- 更新产品扩展表
    UPDATE product_template_expand 
    SET datelastupdate = NOW(), datelastupdate_beijing = NOW() + interval '8h'
    WHERE prod_id = v_Product_Tmpl_Id;
END;
$BODY$
  LANGUAGE plpgsql