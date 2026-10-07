/** Hooks de acceso al estado global. Lanzan si se usan fuera de <Proveedores>. */
import { useContext } from 'react'

import {
  ContextoAvisoReact,
  ContextoChatReact,
  ContextoEstacionReact,
  ContextoTemaReact,
  type ContextoAviso,
  type ContextoChat,
  type ContextoEstacion,
  type ContextoTema,
} from './contextos'

export function useTema(): ContextoTema {
  const ctx = useContext(ContextoTemaReact)
  if (!ctx) throw new Error('useTema debe usarse dentro de <Proveedores>')
  return ctx
}

export function useAviso(): ContextoAviso {
  const ctx = useContext(ContextoAvisoReact)
  if (!ctx) throw new Error('useAviso debe usarse dentro de <Proveedores>')
  return ctx
}

export function useEstacionSeleccionada(): ContextoEstacion {
  const ctx = useContext(ContextoEstacionReact)
  if (!ctx) throw new Error('useEstacionSeleccionada debe usarse dentro de <Proveedores>')
  return ctx
}

export function useChat(): ContextoChat {
  const ctx = useContext(ContextoChatReact)
  if (!ctx) throw new Error('useChat debe usarse dentro de <Proveedores>')
  return ctx
}
