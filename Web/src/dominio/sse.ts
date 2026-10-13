/**
 * Parser incremental de Server-Sent Events leídos con fetch.
 *
 * `EventSource` no permite POST, así que `/chat/stream` se lee del body de un
 * fetch y se interpreta aquí. El parser es un cierre con estado: se alimenta
 * con los trozos según llegan por la red y devuelve los eventos completos que
 * haya en cada momento (un evento puede llegar partido en varios trozos, o
 * varios eventos juntos en uno).
 */
export interface EventoSSE {
  evento: string
  datos: string
}

const FIN_DE_EVENTO = /\r?\n\r?\n/

export function crearParserSSE(): (trozo: string) => EventoSSE[] {
  let pendiente = ''

  return function alimentar(trozo: string): EventoSSE[] {
    pendiente += trozo
    const eventos: EventoSSE[] = []
    for (let corte = FIN_DE_EVENTO.exec(pendiente); corte !== null; corte = FIN_DE_EVENTO.exec(pendiente)) {
      const bloque = pendiente.slice(0, corte.index)
      pendiente = pendiente.slice(corte.index + corte[0].length)
      const evento = interpretarBloque(bloque)
      if (evento) eventos.push(evento)
    }
    return eventos
  }
}

/** Un bloque = las líneas de un evento. Sin líneas `data:` no hay evento (p. ej. solo comentarios). */
function interpretarBloque(bloque: string): EventoSSE | null {
  let evento = 'message'
  const datos: string[] = []
  for (const linea of bloque.split(/\r?\n/)) {
    if (!linea || linea.startsWith(':')) continue
    const separador = linea.indexOf(':')
    const campo = separador === -1 ? linea : linea.slice(0, separador)
    let valor = separador === -1 ? '' : linea.slice(separador + 1)
    if (valor.startsWith(' ')) valor = valor.slice(1)
    if (campo === 'event') evento = valor
    else if (campo === 'data') datos.push(valor)
    // Otros campos del formato (id, retry) no se usan aquí.
  }
  if (datos.length === 0) return null
  return { evento, datos: datos.join('\n') }
}
