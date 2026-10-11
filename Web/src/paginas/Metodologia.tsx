import { BLOQUES, ETIQUETA_BLOQUE, HORAS_BLOQUE, UMBRAL_COBERTURA } from '../dominio/constantes'

const JUSTIFICACION_BLOQUE: Record<string, string> = {
  madrugada: 'Nivel de fondo',
  manana: 'Hora punta, picos de NO2',
  tarde: 'Picos de O3',
  noche: 'Tráfico vespertino',
}

interface Referencia {
  titulo: string
  organismo: string
  url?: string
}

interface Documento {
  titulo: string
  tema: 'salud' | 'normativa' | 'proyecto'
  fuentes: Referencia[]
}

/** Bibliografía del corpus RAG: título y fuentes declaradas en el frontmatter de cada documento (`data/rag/*.md`). */
const CORPUS: Documento[] = [
  {
    titulo: 'Guías de calidad del aire de la OMS (2021)',
    tema: 'normativa',
    fuentes: [
      { titulo: 'WHO global air quality guidelines: PM2.5, PM10, ozone, nitrogen dioxide, sulfur dioxide and carbon monoxide (Ginebra, 2021)', organismo: 'OMS', url: 'https://www.who.int/publications/i/item/9789240034228' },
      { titulo: 'Nota descriptiva «Calidad del aire ambiente (exterior) y salud»', organismo: 'OMS', url: 'https://www.who.int/es/news-room/fact-sheets/detail/ambient-(outdoor)-air-quality-and-health' },
    ],
  },
  {
    titulo: 'Límites legales de calidad del aire en la Unión Europea',
    tema: 'normativa',
    fuentes: [
      { titulo: 'Directiva 2008/50/CE relativa a la calidad del aire ambiente y a una atmósfera más limpia en Europa', organismo: 'Parlamento Europeo y Consejo', url: 'https://eur-lex.europa.eu/legal-content/ES/TXT/?uri=CELEX:32008L0050' },
      { titulo: 'Directiva (UE) 2024/2881 relativa a la calidad del aire ambiente (refundición, límites 2030)', organismo: 'Parlamento Europeo y Consejo', url: 'https://eur-lex.europa.eu/eli/dir/2024/2881/oj' },
      { titulo: 'Real Decreto 102/2011, de 28 de enero, relativo a la mejora de la calidad del aire', organismo: 'Gobierno de España (BOE)', url: 'https://www.boe.es/eli/es/rd/2011/01/28/102' },
      { titulo: 'Sentencia de 2022 sobre superaciones de NO2 en España, asunto C-125/20', organismo: 'Tribunal de Justicia de la Unión Europea' },
    ],
  },
  {
    titulo: 'Protocolo de episodios de alta contaminación de Madrid (NO2)',
    tema: 'normativa',
    fuentes: [
      { titulo: 'Protocolo de actuación para episodios de contaminación por dióxido de nitrógeno (NO2)', organismo: 'Ayuntamiento de Madrid', url: 'https://www.madrid.es' },
      { titulo: 'Madrid 360 y Zona de Bajas Emisiones', organismo: 'Ayuntamiento de Madrid', url: 'https://www.madrid360.es' },
    ],
  },
  {
    titulo: 'Partículas en suspensión (PM10 y PM2.5) y salud',
    tema: 'salud',
    fuentes: [
      { titulo: 'WHO global air quality guidelines 2021, capítulos de PM2.5 y PM10', organismo: 'OMS', url: 'https://www.who.int/publications/i/item/9789240034228' },
      { titulo: 'Informes «Air quality in Europe»', organismo: 'Agencia Europea de Medio Ambiente (EEA)', url: 'https://www.eea.europa.eu' },
      { titulo: 'Información y avisos sobre intrusiones de polvo sahariano (calima)', organismo: 'AEMET', url: 'https://www.aemet.es' },
    ],
  },
  {
    titulo: 'Óxidos de nitrógeno (NO, NO2, NOx) y salud',
    tema: 'salud',
    fuentes: [
      { titulo: 'WHO global air quality guidelines 2021, capítulo de dióxido de nitrógeno', organismo: 'OMS', url: 'https://www.who.int/publications/i/item/9789240034228' },
      { titulo: 'Informes «Air quality in Europe»', organismo: 'Agencia Europea de Medio Ambiente (EEA)', url: 'https://www.eea.europa.eu' },
      { titulo: 'Basic Information about NO2', organismo: 'U.S. Environmental Protection Agency (EPA)', url: 'https://www.epa.gov/no2-pollution' },
    ],
  },
  {
    titulo: 'Ozono troposférico (O3) y salud',
    tema: 'salud',
    fuentes: [
      { titulo: 'WHO global air quality guidelines 2021, capítulo de ozono', organismo: 'OMS', url: 'https://www.who.int/publications/i/item/9789240034228' },
      { titulo: 'Informes «Air quality in Europe»', organismo: 'Agencia Europea de Medio Ambiente (EEA)', url: 'https://www.eea.europa.eu' },
      { titulo: 'Ground-level Ozone Basics', organismo: 'U.S. Environmental Protection Agency (EPA)', url: 'https://www.epa.gov/ground-level-ozone-pollution' },
    ],
  },
  {
    titulo: 'Alergias respiratorias y contaminación en Madrid',
    tema: 'salud',
    fuentes: [
      { titulo: 'Alergia y contaminación (información al paciente)', organismo: 'Sociedad Española de Alergología e Inmunología Clínica (SEAIC)', url: 'https://www.seaic.org' },
      { titulo: 'WHO global air quality guidelines 2021', organismo: 'OMS', url: 'https://www.who.int/publications/i/item/9789240034228' },
      { titulo: 'Air quality in Europe (informes) y literatura revisada sobre contaminación y aeroalérgenos', organismo: 'Agencia Europea de Medio Ambiente (EEA)', url: 'https://www.eea.europa.eu' },
    ],
  },
  {
    titulo: 'Recomendaciones de exposición por perfil de persona',
    tema: 'salud',
    fuentes: [
      { titulo: 'WHO global air quality guidelines 2021 y notas descriptivas sobre calidad del aire y salud', organismo: 'OMS', url: 'https://www.who.int/publications/i/item/9789240034228' },
      { titulo: 'Información al paciente sobre alergia y contaminación', organismo: 'SEAIC', url: 'https://www.seaic.org' },
      { titulo: 'Recomendaciones de salud durante episodios de contaminación', organismo: 'Ayuntamiento de Madrid', url: 'https://www.madrid.es' },
    ],
  },
  {
    titulo: 'Estaciones de medición y zonas de Madrid',
    tema: 'proyecto',
    fuentes: [{ titulo: 'Estaciones de control de calidad del aire (catálogo del portal de datos abiertos)', organismo: 'Ayuntamiento de Madrid', url: 'https://datos.madrid.es' }],
  },
  {
    titulo: 'Glosario de magnitudes y estructura de los datos',
    tema: 'proyecto',
    fuentes: [
      { titulo: 'Especificación de los códigos de magnitud y del formato de los datos de calidad del aire', organismo: 'Ayuntamiento de Madrid', url: 'https://datos.madrid.es' },
      { titulo: 'WHO global air quality guidelines 2021', organismo: 'OMS', url: 'https://www.who.int/publications/i/item/9789240034228' },
    ],
  },
  {
    titulo: 'Aviso médico y sobre el uso de la información',
    tema: 'proyecto',
    fuentes: [
      { titulo: 'WHO global air quality guidelines 2021', organismo: 'OMS', url: 'https://www.who.int/publications/i/item/9789240034228' },
      { titulo: 'Portal de datos abiertos, calidad del aire', organismo: 'Ayuntamiento de Madrid', url: 'https://datos.madrid.es' },
    ],
  },
]

const TEMA_ETIQUETA = { salud: 'Salud', normativa: 'Normativa', proyecto: 'Proyecto' } as const

export default function Metodologia() {
  const umbral = Math.round(UMBRAL_COBERTURA * 100)
  return (
    <div className="prosa">
      <header className="pagina-cabecera">
        <div className="apilado-s">
          <h1>Metodología</h1>
          <p>Qué datos usa el sistema, cómo los resume, qué detecta y qué no puede decir.</p>
        </div>
      </header>

      <h2 id="datos">Origen de los datos</h2>
      <p>
        Las mediciones proceden del <strong>portal de datos abiertos del Ayuntamiento de Madrid</strong>: el histórico
        en ficheros CSV y la API en tiempo real (<code>calair_tiemporeal</code>), que publica para cada estación,
        contaminante y día una fila con 24 columnas horarias y sus marcas de validez. Un pipeline diario transforma ese
        formato ancho en una fila por hora, lo valida y lo agrega por bloques del día. El catálogo de estaciones (nombre,
        dirección, tipo, coordenadas y qué analizadores tiene cada una) también es del Ayuntamiento; el distrito se
        asignó a mano a partir de la dirección.
      </p>
      <p>
        El proyecto se limita a <strong>seis contaminantes</strong> con relevancia sanitaria y cobertura suficiente:
        NO, NO2, NOx, O3, PM10 y PM2.5, todos en µg/m³. No todas las estaciones miden los seis; el selector de cada
        estación solo ofrece los que mide.
      </p>

      <h2 id="bloques">Bloques del día</h2>
      <p>
        Cada día pasa de 24 mediciones a <strong>cuatro bloques</strong>. Un bloque captura el régimen de una franja sin
        diluir los picos, como haría una media diaria. Para cada bloque se guardan media, máximo, mínimo, número de
        horas válidas y cobertura.
      </p>
      <div className="tabla-envoltorio">
        <table className="tabla">
          <thead>
            <tr>
              <th scope="col">Bloque</th>
              <th scope="col">Horas</th>
              <th scope="col">Justificación (análisis exploratorio)</th>
            </tr>
          </thead>
          <tbody>
            {BLOQUES.map((b) => (
              <tr key={b}>
                <td>
                  {ETIQUETA_BLOQUE[b]} <span className="texto-apagado">({b})</span>
                </td>
                <td className="tabular">{HORAS_BLOQUE[b]}</td>
                <td>{JUSTIFICACION_BLOQUE[b]}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="t-s texto-2">
        En la API los bloques se escriben sin tilde: <code>madrugada</code>, <code>manana</code>, <code>tarde</code>,{' '}
        <code>noche</code>.
      </p>

      <h2 id="detector">Detección de anomalías</h2>
      <p>
        El detector principal es un <strong>Isolation Forest por contaminante</strong> (seis modelos, 200 árboles,
        <code>contamination = 0,01</code>, escalado propio por contaminante) entrenado sobre el histórico de bloques.
        Trabaja con tres características comparables entre estaciones: la desviación respecto al valor esperado
        (z-score), la cobertura del bloque y el coeficiente de variación (una franja plana delata un sensor congelado).
        Así cubre tanto anomalías <em>ambientales</em> (niveles inusuales) como <em>operativas</em> (sensor caído o
        congelado).
      </p>
      <p>
        Como referencia se calcula además un <strong>baseline z-score</strong>: el valor esperado y su dispersión por
        estación, contaminante, bloque y mes. Un |z| &gt; 3 marca el bloque como inusual por nivel. El z-score ve los
        picos pero es ciego a los fallos de sensor; el Isolation Forest detecta ambos, y por eso es el detector que
        alimenta <code>is_anomaly</code>.
      </p>
      <p>
        <strong>Cómo leer la salida.</strong> <code>is_anomaly</code> es la decisión del modelo. <code>anomaly_score</code>{' '}
        es el score negado del Isolation Forest: <strong>más alto = más anómalo</strong>, pero <strong>no es una
        probabilidad</strong> ni un porcentaje de confianza y aquí nunca se muestra como tal. La proporción de bloques
        marcados (≈ 1 %) la fija el hiperparámetro <code>contamination</code>, no es un hallazgo del modelo.
      </p>

      <h2 id="cobertura">La regla de cobertura ≥ {umbral} %</h2>
      <p>
        La cobertura es el cociente entre horas válidas y horas del bloque. Cuando faltan muchas horas, el propio hueco
        hace saltar al detector: la primera ejecución sobre datos reales marcó quince bloques y <em>todos</em> eran
        franjas a medio medir con niveles normales. Por eso <strong>una anomalía solo se presenta como tal si la
        cobertura del bloque es al menos del {umbral} %</strong>. Por debajo, esta web muestra el dato con su cobertura
        y lo etiqueta como «cobertura baja»: ni anomalía ni normal, porque no se puede saber. Es una regla de
        correctitud, no de estilo.
      </p>
      <p>
        Por el mismo motivo, los valores ausentes se dibujan como <strong>huecos</strong>, nunca como ceros: un bloque
        sin dato pintado a cero sería una gráfica falsa.
      </p>

      <h2 id="asistente">Cómo funciona el asistente</h2>
      <p>
        El asistente combina dos fuentes y las mantiene separadas: las <strong>cifras</strong> salen de consultas SQL
        predefinidas y parametrizadas sobre la tabla de bloques (el modelo rellena parámetros validados, nunca escribe
        SQL), y las <strong>afirmaciones sobre salud o normativa</strong> salen de un corpus documental propio. Cada
        afirmación documental lleva una cita <code>[D1]</code>, <code>[D2]</code>… que el servicio de evidencias
        verifica contra el corpus antes de mostrarla: las citas se comprueban, no se creen. En la respuesta, las fuentes
        de tipo <em>sql</em> y <em>documento</em> aparecen en grupos distintos.
      </p>
      <p>
        El sistema <strong>no tiene memoria</strong>: cada pregunta se responde de forma independiente y las preguntas
        de seguimiento no funcionan. Tampoco predice: describe el presente y el pasado.
      </p>

      <h2 id="limitaciones">Limitaciones conocidas</h2>
      <ul>
        <li>
          <strong>La hora 23 nunca llega.</strong> El endpoint en tiempo real solo sirve el día en curso y publica con
          una hora de retraso; la ingesta nocturna encuentra, como mucho, la hora 22, y al día siguiente la API ya solo
          devuelve el día nuevo. Consecuencia: el bloque de noche (20–23 h) tiene cobertura 0,75 todos los días. Sigue
          por encima del umbral, pero su media se calcula con tres horas de cuatro.
        </li>
        <li>
          <strong>Los datos del día se consolidan de noche.</strong> El pipeline se ejecuta a las 23:45 con las cuatro
          franjas cerradas; ejecutarlo a media jornada genera falsos positivos transitorios en la franja abierta. Por
          eso la pantalla «Hoy» muestra el último día con datos y lo indica cuando no es el día de hoy.
        </li>
        <li>
          <strong>Sin etiquetas.</strong> No hay anomalías anotadas a mano, así que no se pueden calcular precisión ni
          exhaustividad; la evaluación es cualitativa (casos extremos, cruce con el baseline y eventos conocidos como la
          pirotecnia de Nochevieja).
        </li>
        <li>
          <strong>Baseline con fuga de información.</strong> El valor esperado se calcula con todo el histórico, incluido
          el periodo evaluado, y no pondera la recencia aunque la contaminación baja con los años.
        </li>
        <li>Sin datos meteorológicos, de polen ni de aire interior; sin predicción.</li>
        <li>Distritos asignados manualmente; cuatro estaciones en frontera entre distritos están pendientes de revisar.</li>
        <li>
          El umbral de evidencia del asistente documental está calibrado a mano y todavía sin casos de evaluación; ante
          la duda, el asistente se abstiene.
        </li>
      </ul>

      <h2 id="aviso">Aviso médico y sobre el uso de la información</h2>
      <p>
        Este sistema ofrece <strong>información general de carácter educativo e informativo</strong> sobre la calidad
        del aire de Madrid y sus efectos en la salud. <strong>No constituye consejo médico</strong> ni un diagnóstico, y{' '}
        <strong>no sustituye</strong> la consulta con un profesional sanitario.
      </p>
      <ul>
        <li>
          Las recomendaciones son orientativas y de salud pública, no pautas clínicas individualizadas. La respuesta de
          cada persona a la contaminación varía según su edad, estado de salud, medicación y otros factores.
        </li>
        <li>
          Ante síntomas (dificultad para respirar, dolor torácico, crisis de asma, etc.), consulta con un profesional
          sanitario o, si es urgente, con los servicios de emergencia (112).
        </li>
        <li>
          Las personas con enfermedades respiratorias o cardiovasculares deben seguir las indicaciones de su médico, que
          prevalecen sobre cualquier recomendación general de este asistente.
        </li>
        <li>
          Los datos de contaminación proceden del Ayuntamiento de Madrid y pueden contener errores, huecos o retrasos. El
          modelo de anomalías es una ayuda estadística, no una medición oficial. Este sistema no sustituye a los avisos
          y protocolos oficiales.
        </li>
        <li>
          El corpus documental es una síntesis divulgativa elaborada para el proyecto a partir de fuentes públicas y
          autorizadas. Las cifras y umbrales deben verificarse en los documentos originales, que pueden actualizarse.
        </li>
      </ul>

      <h2 id="bibliografia">Bibliografía del corpus documental</h2>
      <p className="t-s texto-2">
        Once documentos escritos para el proyecto como síntesis de fuentes públicas. Cada uno declara sus fuentes en su
        cabecera; son las que se listan aquí.
      </p>
      <ol>
        {CORPUS.map((d) => (
          <li key={d.titulo}>
            <strong>{d.titulo}</strong> <span className="chip">{TEMA_ETIQUETA[d.tema]}</span>
            <ul className="t-s texto-2" style={{ marginTop: 4 }}>
              {d.fuentes.map((f) => (
                <li key={f.titulo}>
                  {f.titulo} — {f.organismo}
                  {f.url && (
                    <>
                      {' '}
                      ·{' '}
                      <a href={f.url} target="_blank" rel="noopener noreferrer">
                        {f.url.replace(/^https?:\/\//, '')}
                      </a>
                    </>
                  )}
                </li>
              ))}
            </ul>
          </li>
        ))}
      </ol>
    </div>
  )
}
