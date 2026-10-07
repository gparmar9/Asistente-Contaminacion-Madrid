import { mensajeErrorChat } from '../../api/errores'
import type { ConsultaChat } from '../../dominio/chat'
import { Espera } from './Espera'
import { Fuentes } from './Fuentes'
import { TextoConCitas } from './TextoConCitas'

interface Props {
  consulta: ConsultaChat
  onReintentar?: (consulta: ConsultaChat) => void
}

/** Una pregunta y su respuesta: unidad independiente (el sistema no tiene memoria). */
export function Consulta({ consulta, onReintentar }: Props) {
  const { id, numero, pregunta, estado } = consulta
  return (
    <article className="consulta" aria-labelledby={`consulta-${id}`}>
      <h3 id={`consulta-${id}`} className="visualmente-oculto">
        Consulta {numero}
      </h3>
      <div className="pregunta">{pregunta}</div>

      <div className="respuesta">
        {estado.tipo === 'esperando' && <Espera activo />}

        {estado.tipo === 'error' && (
          <div className="aviso-bloque aviso-error" role="alert">
            <strong>{mensajeErrorChat(estado.error).titulo}.</strong> {mensajeErrorChat(estado.error).detalle}
            {onReintentar && (
              <>
                {' '}
                <button type="button" className="boton boton-s" style={{ marginLeft: 8 }} onClick={() => onReintentar(consulta)}>
                  Reintentar
                </button>
              </>
            )}
          </div>
        )}

        {estado.tipo === 'ok' && (
          <div className="tarjeta">
            <TextoConCitas texto={estado.respuesta.respuesta} fuentes={estado.respuesta.fuentes ?? []} idRespuesta={id} />
            <Fuentes fuentes={estado.respuesta.fuentes ?? []} texto={estado.respuesta.respuesta} idRespuesta={id} />
            {estado.respuesta.advertencia && (
              <p className="t-xs texto-2" style={{ marginTop: 12 }}>
                {estado.respuesta.advertencia}
              </p>
            )}
          </div>
        )}
      </div>
    </article>
  )
}
