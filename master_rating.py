homologacion_pais = spark.read.options(header='True',inferSchema='True', delimiter='|').csv("/home/data/out/homologacion_pais.csv")
homologacion_rating = spark.read.options(header='True',inferSchema='True', delimiter='|').csv("/home/data/out/homologacion_rating.csv")
rating_empresa = spark.read.options(header='True',inferSchema='True', delimiter='|').csv("/home/data/out/rating_empresa.csv")
rating_soberano = spark.read.options(header='True',inferSchema='True', delimiter='|').csv("/home/data/out/rating_soberano.csv")

rating_empresa.createOrReplaceTempView("RE")
rating_soberano.createOrReplaceTempView("RS")
homologacion_pais.createOrReplaceTempView("HP")
homologacion_rating.createOrReplaceTempView("HR")

re_hp = spark.sql("""
  SELECT
    re.rut,
    re.dv,
    re.nombre,
    hp.pais,
    hp.pais_bbg,
    re.mdy,
    re.sp,
    re.fitch
  FROM
    RE re
      INNER JOIN HP hp
        ON re.pais_bbg == hp.pais_bbg
""")

re_hp.createOrReplaceTempView("RE_HP")

rehp_hr = spark.sql("""
  SELECT
    rehp.rut,
    rehp.dv,
    rehp.nombre,
    rehp.pais,
    rehp.pais_bbg,
    hr.rating,
    hr.rating_norma AS rating_empresa,
    hr.agencia_homol,
    hr.orden_norma ,
    ROW_NUMBER() OVER(PARTITION BY rehp.rut ORDER BY hr.orden_norma DESC) as row_number
  FROM
    RE_HP rehp
      LEFT JOIN HR hr
        ON ('MDY' = hr.agencia_homol AND rehp.mdy = hr.rating)
        OR ('SP' = hr.agencia_homol AND rehp.sp = hr.rating)
        OR ('FITCH' = hr.agencia_homol AND rehp.fitch = hr.rating)
""")

rehp_hr.createOrReplaceTempView("REHP_HR")

rs_hp_rehphr_hr = spark.sql("""
  SELECT
    rs.pais_bbg,
    hr.rating_norma AS rating_soberano,
    ROW_NUMBER() OVER(PARTITION BY hp.pais ORDER BY hp.pais DESC) AS row_number_2
  FROM
    RS rs
      INNER JOIN HP hp
        ON rs.pais_bbg == hp.pais_bbg
      INNER JOIN REHP_HR rehp_hr
        ON hp.pais = rehp_hr.pais
      LEFT JOIN HR hr
        ON (rehp_hr.agencia_homol = hr.agencia_homol AND rs.mdy = hr.rating)
        OR (rehp_hr.agencia_homol = hr.agencia_homol AND rs.sp = hr.rating)
        OR (rehp_hr.agencia_homol = hr.agencia_homol AND rs.fitch = hr.rating)
  ORDER BY
    rehp_hr.rut ASC
""").filter('row_number_2 == 1')

rs_hp_rehphr_hr.createOrReplaceTempView("RS_HP_REHPHR_HR")

result = spark.sql("""
  SELECT
    rehp_hr.rut,
    rehp_hr.dv,
    rehp_hr.nombre,
    rehp_hr.pais,
    rehp_hr.rating_empresa,
    rs_hp_rehphr_hr.rating_soberano
  FROM
    REHP_HR rehp_hr
      INNER JOIN RS_HP_REHPHR_HR rs_hp_rehphr_hr
        ON rehp_hr.pais_bbg = RS_HP_REHPHR_HR.pais_bbg
  WHERE
    rehp_hr.row_number == 1
""")

result.show(1000, False)