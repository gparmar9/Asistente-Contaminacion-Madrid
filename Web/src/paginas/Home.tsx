import { Link } from 'react-router-dom'

import { useChat } from '../estado/hooks'

/**
 * Portada tipo landing: qué ofrece el proyecto y a dónde ir, en tono amable.
 * Las cifras son reales (catálogo y alcance del pipeline); el marketing está
 * en el tono, no en los datos.
 */
export default function Home() {
  const { abrir } = useChat()

  return (
    <div className="portada">
      <section className="portada-hero" aria-label="Presentación">
        <img src="/img/hero-parque.jpg" alt="" className="portada-hero-fondo" />
        <div className="portada-hero-velo" aria-hidden="true" />
        <div className="portada-hero-contenido">
          <span className="portada-insignia">🌿 Calidad del aire de Madrid, cada día</span>
          <h1 className="portada-titulo">
            Respira mejor.
            <br />
            Elige tu zona.
          </h1>
          <p className="portada-sub">
            Descubre cómo está el aire en tu barrio, qué estaciones detectan algo fuera de lo normal
            y qué significa para tu salud. Pregunta con tus palabras: el asistente responde con los
            datos oficiales del Ayuntamiento.
          </p>
          <div className="portada-acciones">
            <button type="button" className="boton boton-primario boton-l" onClick={abrir}>
              💬 Probar el asistente
            </button>
            <Link to="/estadisticas" className="boton boton-l portada-boton-claro">
              Ver las estadísticas
            </Link>
          </div>
          <ul className="portada-stats">
            <li>
              <strong>24</strong>
              <span>estaciones de control</span>
            </li>
            <li>
              <strong>6</strong>
              <span>contaminantes vigilados</span>
            </li>
            <li>
              <strong>4</strong>
              <span>franjas del día analizadas</span>
            </li>
          </ul>
        </div>
      </section>

      <section aria-labelledby="portada-rasgos-titulo">
        <header className="portada-seccion-cabecera">
          <h2 id="portada-rasgos-titulo">Un vistazo y lo sabes</h2>
          <p>Tres maneras de entender el aire que respiras.</p>
        </header>
        <div className="portada-rasgos">
          <button type="button" className="tarjeta portada-rasgo" onClick={abrir}>
            <span className="portada-rasgo-icono" aria-hidden="true">
              💬
            </span>
            <h3>Pregunta con tus palabras</h3>
            <p>«¿Cómo está hoy el aire en el Retiro?» El asistente contesta con los datos y cita sus fuentes.</p>
          </button>
          <Link to="/estaciones" className="tarjeta portada-rasgo">
            <span className="portada-rasgo-icono" aria-hidden="true">
              📍
            </span>
            <h3>Mapa de estaciones</h3>
            <p>Las 24 estaciones de control sobre el mapa: elige la más cercana y entra en su detalle.</p>
          </Link>
          <Link to="/estadisticas" className="tarjeta portada-rasgo">
            <span className="portada-rasgo-icono" aria-hidden="true">
              📈
            </span>
            <h3>Anomalías detectadas con IA</h3>
            <p>Un modelo aprende lo normal de cada estación y marca los bloques que se salen de lo esperado.</p>
          </Link>
        </div>
      </section>

      <section className="tarjeta portada-zona" aria-labelledby="portada-zona-titulo">
        <div className="portada-zona-texto">
          <h2 id="portada-zona-titulo">Visualiza dónde respira mejor la ciudad</h2>
          <p>
            Cada estación cuenta su propia historia: tráfico, parques, periferia. Compara zonas, sigue
            la evolución por franjas del día y localiza los episodios de contaminación de un vistazo.
          </p>
          <Link to="/estaciones" className="boton boton-primario">
            Explorar el mapa →
          </Link>
        </div>
        <img
          src="/img/vista-aire.jpg"
          alt="Ilustración de Madrid a vista de pájaro con zonas coloreadas según la calidad del aire"
          className="portada-zona-img"
        />
      </section>
    </div>
  )
}
