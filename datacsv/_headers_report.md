# Reporte de cabeceras CSV

- Carpeta analizada: `datacsv`
- Archivos encontrados: **14**

## Resumen por cantidad de columnas

| columnas | archivos |
|---:|---:|
| 25 | 1 |
| 22 | 1 |
| 21 | 1 |
| 20 | 1 |
| 18 | 3 |
| 17 | 7 |

## Unión de columnas (normalizadas)

Total: **51**

```
areag_ocu, año_ocu, año_ocu_1, causa_acc, color_veh, color_vehi, condicion_pil, corre_base, depto_ocu, dia_ocu, dia_sem_ocu, día_ocu, día_sem_ocu, edad_con, edad_m1, edad_per, edad_pil, edad_quinquenales, estado_con, estado_pil, g_edad, g_edad_2, g_edad_60ymás, g_edad_80ymás, g_edad_pil, g_hora, g_hora_5, g_modelo_veh, hora_ocu, marca_veh, marca_vehi, mayor_menor, mes_ocu, modelo_veh, modelo_vehi, muni_ocu, mupio_ocu, num_corre, num_correlativo, num_hecho, núm_corre, sexo_con, sexo_per, sexo_pil, tipo_eve, tipo_veh, tipo_vehi, tipo_vehiculo, zona_ciudad, zona_ocu, área_geo_ocu
```

## Intersección de columnas (normalizadas)

Total: **4**

```
año_ocu, depto_ocu, hora_ocu, mes_ocu
```

## Detalle por archivo

| archivo | #cols | headers (originales) |
|---|---:|---|
| `2015.csv` | 25 | núm_corre|año_ocu|mes_ocu|día_ocu|día_sem_ocu|hora_ocu|g_hora|g_hora_5|depto_ocu|mupio_ocu|área_geo_ocu|zona_ocu|sexo_per|edad_per|mayor_menor|g_edad_80ymás|g_edad_60ymás|edad_quinquenales|estado_con|tipo_veh|marca_veh|color_veh|modelo_veh|g_modelo_veh|tipo_eve |
| `2014.csv` | 22 | num_correlativo|corre_base|día_ocu|día_sem_ocu|hora_ocu|g_hora|mes_ocu|depto_ocu|mupio_ocu|zona_ocu|área_geo_ocu|sexo_con|edad_con|g_edad|mayor_menor|estado_con|tipo_veh|modelo_veh|color_veh|marca_veh|tipo_eve|año_ocu |
| `2013.csv` | 21 | num_hecho|dia_ocu|mes_ocu|dia_sem_ocu|hora_ocu|g_hora|depto_ocu|mupio_ocu|areag_ocu|zona_ocu|sexo_pil|edad_pil|g_edad_2|mayor_menor|tipo_veh|color_veh|modelo_veh|causa_acc|marca_veh|estado_pil|año_ocu |
| `2011.csv` | 20 | num_hecho|dia_ocu|mes_ocu|dia_sem_ocu|hora_ocu|depto_ocu|muni_ocu|areag_ocu|zona_ocu|sexo_pil|edad_pil|g_edad_pil|edad_m1|estado_pil|tipo_vehiculo|marca_vehi|color_vehi|modelo_vehi|causa_acc|año_ocu |
| `2012.csv` | 18 | num_hecho|dia_ocu|mes_ocu|dia_sem_ocu|hora_ocu|depto_ocu|mupio_ocu|areag_ocu|zona_ocu|sexo_pil|edad_pil|g_edad|edad_m1|condicion_pil|tipo_vehi|color_vehi|causa_acc|año_ocu |
| `2016.csv` | 18 | núm_corre|día_ocu|año_ocu|hora_ocu|mes_ocu|día_sem_ocu|mupio_ocu|depto_ocu|área_geo_ocu|zona_ocu|tipo_veh|marca_veh|color_veh|modelo_veh|tipo_eve|g_hora|g_hora_5|g_modelo_veh |
| `2021.csv` | 18 | núm_corre|año_ocu|día_ocu|hora_ocu|g_hora|g_hora_5|mes_ocu|día_sem_ocu|depto_ocu|mupio_ocu|zona_ocu|zona_ciudad|tipo_veh|marca_veh|color_veh|modelo_veh|g_modelo_veh|tipo_eve |
| `2009.csv` | 17 | num_hecho|dia_ocu|mes_ocu|año_ocu|dia_sem_ocu|hora_ocu|depto_ocu|areag_ocu|sexo_pil|edad_pil|g_edad_pil|estado_pil|tipo_vehi|color_vehi|modelo_vehi|causa_acc|año_ocu.1 |
| `2010.csv` | 17 | num_hecho|dia_ocu|mes_ocu|año_ocu|dia_sem_ocu|hora_ocu|depto_ocu|areag_ocu|sexo_pil|edad_pil|g_edad_pil|estado_pil|tipo_vehi|color_vehi|modelo_vehi|causa_acc|año_ocu.1 |
| `2017.csv` | 17 | núm_corre|año_ocu|día_ocu|hora_ocu|g_hora|g_hora_5|mes_ocu|día_sem_ocu|mupio_ocu|depto_ocu|zona_ocu|tipo_veh|marca_veh|color_veh|modelo_veh|g_modelo_veh|tipo_eve |
| `2018__Sheet1.csv` | 17 | núm_corre|año_ocu|día_ocu|hora_ocu|g_hora|g_hora_5|mes_ocu|día_sem_ocu|mupio_ocu|depto_ocu|zona_ocu|tipo_veh|marca_veh|color_veh|modelo_veh|g_modelo_veh|tipo_eve |
| `20199__Sheet1.csv` | 17 | núm_corre|año_ocu|día_ocu|hora_ocu|g_hora|g_hora_5|mes_ocu|día_sem_ocu|depto_ocu|mupio_ocu|zona_ocu|tipo_veh|marca_veh|color_veh|modelo_veh|g_modelo_veh|tipo_eve |
| `2022__Sheet1.csv` | 17 | num_corre|año_ocu|día_ocu|hora_ocu|g_hora|g_hora_5|mes_ocu|día_sem_ocu|depto_ocu|mupio_ocu|zona_ocu|tipo_veh|marca_veh|color_veh|modelo_veh|g_modelo_veh|tipo_eve |
| `20233__Sheet1.csv` | 17 | núm_corre|año_ocu|día_ocu|hora_ocu|g_hora|g_hora_5|mes_ocu|día_sem_ocu|depto_ocu|mupio_ocu|zona_ocu|tipo_veh|marca_veh|color_veh|modelo_veh|g_modelo_veh|tipo_eve |
