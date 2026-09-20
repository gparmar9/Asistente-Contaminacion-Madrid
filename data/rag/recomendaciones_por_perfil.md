---
titulo: Recomendaciones de exposición por perfil de persona
tema: salud
contaminantes: [NO2, PM10, PM2.5, O3]
fuente: "OMS 2021"
---

# Recomendaciones de exposición por perfil de persona

Guía práctica para adaptar la respuesta del asistente al perfil del usuario. La regla general:
identificar qué contaminante está elevado ese día y en esa zona, y cruzarlo con la
sensibilidad del perfil.

## Personas con alergias respiratorias (rinitis, asma alérgica)

- Contaminantes clave: **NO2 y partículas (PM10/PM2.5)**, que potencian la respuesta alérgica
  y agravan los síntomas cuando coinciden con polen alto.
- Mejor zona: **parques y zonas verdes alejadas del tráfico** (interior del Retiro, Casa de
  Campo, Madrid Río) frente a ejes de tráfico.
- Mejor momento: evitar las horas punta de tráfico (7-10h y 19-22h) por el NO2; en días de
  calima, reducir la actividad exterior y ventilar poco.
- El asistente debe recomendar la estación/zona con menor NO2 y PM del día.

## Personas con asma o EPOC

- Sensibles a **NO2, partículas y O3**. En verano el O3 es el factor limitante; en invierno,
  NO2 y partículas.
- Llevar siempre la medicación de rescate. Evitar ejercicio intenso al aire libre cuando el
  contaminante relevante esté en nivel moderado-alto.

## Deportistas (población sana haciendo ejercicio al aire libre)

- El ejercicio multiplica el aire inhalado, así que la exposición efectiva es mucho mayor.
- **En verano**: correr/ciclismo a primera hora de la mañana (O3 en mínimo). Evitar 14-20h.
- **En invierno**: evitar hora punta de tráfico; mediodía suele tener menos NO2.
- Preferir parques grandes frente a circular junto a vías principales.

## Niños, embarazadas y personas mayores

- Grupo sensible general. Priorizar zonas verdes y horas valle.
- En episodios (calima, alerta de ozono o NO2): limitar el tiempo al aire libre y la actividad
  física intensa; en interiores mantener buena calidad del aire.

## Población general sin factores de riesgo

- Con niveles buenos o aceptables, sin restricciones.
- Solo en episodios altos conviene moderar el ejercicio intenso prolongado al aire libre.

## Cómo usar el modelo de anomalías en las recomendaciones

Cuando un bloque está marcado como anomalía (`is_anomaly = true`) significa que es **inusual
respecto al histórico** de esa estación, contaminante, franja y mes (`expected_value`), no
necesariamente que sea peligroso. Puede deberse a un nivel ambiental atípico (comparar `media`
frente a `expected_value`: al alza = episodio de contaminación, lo relevante para salud) o a un
fallo del sensor (poca cobertura de horas o señal congelada). Ante una anomalía **al alza**,
conviene ser más prudente en la recomendación aunque el valor absoluto no sea extremo, y avisar
al usuario de que la situación es atípica.

## Fuentes

- OMS. *WHO global air quality guidelines* (2021) y notas descriptivas sobre calidad del aire y
  salud. <https://www.who.int/publications/i/item/9789240034228>
- Sociedad Española de Alergología e Inmunología Clínica (SEAIC) — información al paciente sobre
  alergia y contaminación. <https://www.seaic.org>
- Ayuntamiento de Madrid — recomendaciones de salud durante episodios de contaminación.
  <https://www.madrid.es>

> Las recomendaciones por perfil son una **síntesis práctica de salud pública** orientativa, no
> pautas clínicas individualizadas.

> Este documento es una **síntesis divulgativa** elaborada para el proyecto a partir de las fuentes citadas; no reproduce literalmente su contenido. Verifica las cifras exactas y su vigencia en los documentos originales antes de cualquier uso operativo. Consulta también el [aviso médico](aviso_medico.md).
