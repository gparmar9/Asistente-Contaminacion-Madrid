/** Contextos de estado global (tema, texto del aviso, estación seleccionada, chat). Los proveedores viven en Proveedores.tsx y los hooks en hooks.ts. */
import { createContext } from 'react'

import type { ConsultaChat } from '../dominio/chat'

export type Tema = 'claro' | 'oscuro'

export interface ContextoTema {
  tema: Tema
  alternar: () => void
}

export interface ContextoAviso {
  texto: string
  /** Actualiza el banner con la advertencia de una respuesta (null/undefined = volver al texto fijo). */
  registrarAdvertencia: (advertencia: string | null | undefined) => void
}

export interface ContextoEstacion {
  codigo: number | undefined
  seleccionar: (codigo: number) => void
}

export interface ContextoChat {
  consultas: readonly ConsultaChat[]
  /** Hay una petición en vuelo: el envío se bloquea (evita doble gasto de LLM). */
  enviando: boolean
  /** Envía una pregunta; con `idExistente` reintenta una consulta fallida. */
  enviar: (pregunta: string, idExistente?: string) => void
  vaciar: () => void
  /** Panel flotante abierto o cerrado. */
  abierto: boolean
  abrir: () => void
  cerrar: () => void
  alternar: () => void
  /** Ha llegado una respuesta mientras el panel estaba cerrado. */
  hayNovedades: boolean
  marcarVisto: () => void
}

export const ContextoTemaReact = createContext<ContextoTema | null>(null)
export const ContextoAvisoReact = createContext<ContextoAviso | null>(null)
export const ContextoEstacionReact = createContext<ContextoEstacion | null>(null)
export const ContextoChatReact = createContext<ContextoChat | null>(null)
