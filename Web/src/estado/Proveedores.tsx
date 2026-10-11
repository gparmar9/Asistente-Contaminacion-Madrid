/**
 * Proveedores de estado global.
 *
 * - Tema claro/oscuro: el valor manual se guarda en localStorage; sin valor
 *   guardado se sigue la preferencia del sistema (index.html aplica el mismo
 *   criterio antes del primer pintado para evitar el destello).
 * - Aviso médico: texto del banner, tomado de `advertencia` en /chat o el fijo.
 * - Estación seleccionada: compartida entre Hoy y Estaciones, recordada en localStorage.
 * - Chat: la conversación de la sesión y el estado del panel flotante. Vive aquí
 *   para que la lista sobreviva al cambiar de pantalla y una petición en vuelo no
 *   se pierda al cerrar el panel. La respuesta llega por /chat/stream: fases de
 *   progreso y, con el agente conectado, el texto token a token.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'

import { preguntarStream } from '../api/consultas'
import type { ConsultaChat, EstadoConsulta } from '../dominio/chat'
import { AVISO_MEDICO_POR_DEFECTO } from '../dominio/constantes'
import { ContextoAvisoReact, ContextoChatReact, ContextoEstacionReact, ContextoTemaReact, type Tema } from './contextos'
import { useAviso } from './hooks'

const CLAVE_TEMA = 'tema'
const CLAVE_ESTACION = 'estacion'

function temaInicial(): Tema {
  const actual = document.documentElement.dataset.theme
  if (actual === 'dark') return 'oscuro'
  if (actual === 'light') return 'claro'
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'oscuro' : 'claro'
}

export function TemaProvider({ children }: { children: ReactNode }) {
  const [tema, setTema] = useState<Tema>(temaInicial)

  useEffect(() => {
    document.documentElement.dataset.theme = tema === 'oscuro' ? 'dark' : 'light'
  }, [tema])

  const alternar = useCallback(() => {
    setTema((anterior) => {
      const siguiente: Tema = anterior === 'oscuro' ? 'claro' : 'oscuro'
      try {
        localStorage.setItem(CLAVE_TEMA, siguiente)
      } catch {
        // Sin almacenamiento (modo privado): el tema dura la sesión.
      }
      return siguiente
    })
  }, [])

  const valor = useMemo(() => ({ tema, alternar }), [tema, alternar])
  return <ContextoTemaReact.Provider value={valor}>{children}</ContextoTemaReact.Provider>
}

export function AvisoProvider({ children }: { children: ReactNode }) {
  const [texto, setTexto] = useState(AVISO_MEDICO_POR_DEFECTO)

  const registrarAdvertencia = useCallback((advertencia: string | null | undefined) => {
    setTexto(advertencia && advertencia.trim() ? advertencia.trim() : AVISO_MEDICO_POR_DEFECTO)
  }, [])

  const valor = useMemo(() => ({ texto, registrarAdvertencia }), [texto, registrarAdvertencia])
  return <ContextoAvisoReact.Provider value={valor}>{children}</ContextoAvisoReact.Provider>
}

function estacionGuardada(): number | undefined {
  try {
    const valor = localStorage.getItem(CLAVE_ESTACION)
    const numero = valor === null ? NaN : Number(valor)
    return Number.isInteger(numero) ? numero : undefined
  } catch {
    return undefined
  }
}

export function EstacionProvider({ children }: { children: ReactNode }) {
  const [codigo, setCodigo] = useState<number | undefined>(estacionGuardada)

  useEffect(() => {
    if (codigo === undefined) return
    try {
      localStorage.setItem(CLAVE_ESTACION, String(codigo))
    } catch {
      // Sin almacenamiento: la selección dura la sesión.
    }
  }, [codigo])

  const seleccionar = useCallback((nuevo: number) => setCodigo(nuevo), [])
  const valor = useMemo(() => ({ codigo, seleccionar }), [codigo, seleccionar])
  return <ContextoEstacionReact.Provider value={valor}>{children}</ContextoEstacionReact.Provider>
}

/** `?chat=abierto` en la URL abre el panel flotante al cargar (enlaces directos al asistente). */
function panelAbiertoInicial(): boolean {
  return new URLSearchParams(window.location.search).get('chat') === 'abierto'
}

export function ChatProvider({ children }: { children: ReactNode }) {
  const [consultas, setConsultas] = useState<ConsultaChat[]>([])
  const [enviando, setEnviando] = useState(false)
  const [abierto, setAbierto] = useState<boolean>(panelAbiertoInicial)
  const [hayNovedades, setHayNovedades] = useState(false)
  // Espejo del estado "abierto" para leerlo desde los callbacks de la petición sin cerrar sobre un valor viejo.
  const abiertoRef = useRef(abierto)
  useEffect(() => {
    abiertoRef.current = abierto
  }, [abierto])
  // Espejo de "enviando" por la misma razón: el guard corre dentro de un callback estable.
  const enviandoRef = useRef(false)
  // La conversación: el agente recuerda los turnos con este id. null = la próxima pregunta estrena sesión.
  const sesionRef = useRef<string | null>(null)

  const { registrarAdvertencia } = useAviso()

  const enviar = useCallback(
    (pregunta: string, idExistente?: string) => {
      if (enviandoRef.current) return
      enviandoRef.current = true
      setEnviando(true)
      const id = idExistente ?? crypto.randomUUID()
      const actualizar = (estado: EstadoConsulta) =>
        setConsultas((lista) => lista.map((c) => (c.id === id ? { ...c, estado } : c)))
      setConsultas((lista) => {
        if (lista.some((c) => c.id === id)) {
          return lista.map((c) => (c.id === id ? { ...c, estado: { tipo: 'esperando' } } : c))
        }
        return [...lista, { id, numero: lista.length + 1, pregunta, estado: { tipo: 'esperando' } }]
      })

      let texto = ''
      preguntarStream(pregunta, sesionRef.current, {
        // La fase solo tiene sentido mientras no hay texto: los tokens ya son la respuesta.
        alCambiarFase: (fase) => {
          if (texto === '') actualizar({ tipo: 'esperando', fase })
        },
        alRecibirTexto: (fragmento) => {
          texto += fragmento
          actualizar({ tipo: 'recibiendo', texto })
        },
      })
        .then((respuesta) => {
          sesionRef.current = respuesta.session_id ?? sesionRef.current
          registrarAdvertencia(respuesta.advertencia)
          actualizar({ tipo: 'ok', respuesta })
          if (!abiertoRef.current) setHayNovedades(true)
        })
        .catch((error: unknown) => {
          actualizar({ tipo: 'error', error })
          if (!abiertoRef.current) setHayNovedades(true)
        })
        .finally(() => {
          enviandoRef.current = false
          setEnviando(false)
        })
    },
    [registrarAdvertencia],
  )

  // Nueva conversación: se vacía la lista y el agente dejará de recordar lo anterior.
  const vaciar = useCallback(() => {
    setConsultas([])
    sesionRef.current = null
  }, [])
  const marcarVisto = useCallback(() => setHayNovedades(false), [])
  const abrir = useCallback(() => {
    setAbierto(true)
    setHayNovedades(false)
  }, [])
  const cerrar = useCallback(() => setAbierto(false), [])
  const alternar = useCallback(() => {
    if (abiertoRef.current) {
      setAbierto(false)
    } else {
      setAbierto(true)
      setHayNovedades(false)
    }
  }, [])

  const valor = useMemo(
    () => ({ consultas, enviando, enviar, vaciar, abierto, abrir, cerrar, alternar, hayNovedades, marcarVisto }),
    [consultas, enviando, enviar, vaciar, abierto, abrir, cerrar, alternar, hayNovedades, marcarVisto],
  )
  return <ContextoChatReact.Provider value={valor}>{children}</ContextoChatReact.Provider>
}

/** Todos los proveedores de estado de la aplicación, en orden (el chat necesita el aviso). */
export function Proveedores({ children }: { children: ReactNode }) {
  return (
    <TemaProvider>
      <AvisoProvider>
        <EstacionProvider>
          <ChatProvider>{children}</ChatProvider>
        </EstacionProvider>
      </AvisoProvider>
    </TemaProvider>
  )
}
