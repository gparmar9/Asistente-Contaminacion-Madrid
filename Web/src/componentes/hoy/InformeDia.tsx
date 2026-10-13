import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'

import { preguntar } from '../../api/consultas'
import { mensajeErrorChat } from '../../api/errores'
import type { Estacion } from '../../api/tipos'
import { PROMPTS_INFORME } from '../../dominio/constantes'
import { desdeISO } from '../../dominio/fechas'
import { useAviso } from '../../estado/hooks'
import { Espera } from '../chat/Espera'
import { Fuentes } from '../chat/Fuentes'
import { TextoConCitas } from '../chat/TextoConCitas'
import { Tarjeta } from '../ui/Tarjeta'

interface Props {
  estacion: Estacion
  /** Día de referencia (hoy, o el último con datos). */
  fecha: string
}

function diaDelAno(iso: string): number {
  const f = desdeISO(iso)
  const inicio = new Date(f.getFullYear(), 0, 1)
  return Math.floor((f.getTime() - inicio.getTime()) / 86_400_000)
}

/**
 * "Informe del día": una pregunta al asistente con un prompt que rota por fecha.
 * Es una llamada a /chat como cualquier otra (sin memoria), cacheada por día,
 * estación y prompt para no gastar LLM en cada render.
 */
export function InformeDia({ estacion, fecha }: Props) {
  const total = PROMPTS_INFORME.length
  const [desplazamiento, setDesplazamiento] = useState(0)
  const indice = (diaDelAno(fecha) + desplazamiento) % total
  const pregunta = PROMPTS_INFORME[indice].replace('{estacion}', estacion.nombre)
  const { registrarAdvertencia } = useAviso()

  const informe = useQuery({
    queryKey: ['informe', fecha, estacion.codigo_corto, indice],
    queryFn: async () => {
      const respuesta = await preguntar(pregunta)
      registrarAdvertencia(respuesta.advertencia)
      return respuesta
    },
    staleTime: Infinity,
    gcTime: 60 * 60_000,
    retry: 0,
  })

  const idRespuesta = `informe-${estacion.codigo_corto}-${indice}`

  return (
    <Tarjeta
      titulo="Informe del día"
      sub={
        <>
          Generado por el asistente. Pregunta: <em>«{pregunta}»</em>
        </>
      }
      acciones={
        <div className="fila" style={{ gap: 6 }}>
          <button type="button" className="boton boton-s" onClick={() => setDesplazamiento((d) => d + 1)} disabled={informe.isFetching}>
            Otro enfoque
          </button>
          <button type="button" className="boton boton-suave boton-s" onClick={() => void informe.refetch()} disabled={informe.isFetching}>
            Regenerar
          </button>
        </div>
      }
    >
      {informe.isPending && <Espera activo texto="Redactando el informe" />}
      {informe.isError && (
        <div className="aviso-bloque aviso-error" role="alert">
          <strong>{mensajeErrorChat(informe.error).titulo}.</strong> {mensajeErrorChat(informe.error).detalle}
        </div>
      )}
      {informe.data && (
        <div className={informe.isFetching ? 'atenuado' : undefined}>
          <TextoConCitas texto={informe.data.respuesta} fuentes={informe.data.fuentes ?? []} idRespuesta={idRespuesta} />
          <Fuentes fuentes={informe.data.fuentes ?? []} texto={informe.data.respuesta} idRespuesta={idRespuesta} />
        </div>
      )}
    </Tarjeta>
  )
}
