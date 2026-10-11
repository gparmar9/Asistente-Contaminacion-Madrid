"""Un turno a la vez por sesión, con eventos en lugar de esperas por tiempo."""
import asyncio

import pytest

from agente.business.sesiones import Sesiones

pytestmark = pytest.mark.anyio


class _Turno:
    """Un turno que entra en la sesión, avisa (`entro`) y no sale hasta que se le deja (`salir`)."""

    def __init__(self, sesiones: Sesiones, session_id: str):
        self.entro = asyncio.Event()
        self.salir = asyncio.Event()
        self.tarea = asyncio.create_task(self._correr(sesiones, session_id))

    async def _correr(self, sesiones: Sesiones, session_id: str) -> None:
        async with sesiones.turno(session_id):
            self.entro.set()
            await self.salir.wait()

    async def terminar(self) -> None:
        self.salir.set()
        await self.tarea


async def _entra(turno: _Turno) -> None:
    await asyncio.wait_for(turno.entro.wait(), timeout=1)


async def test_misma_sesion_espera_y_otra_no():
    sesiones = Sesiones()
    a1 = _Turno(sesiones, "s1")
    await _entra(a1)
    a2, b = _Turno(sesiones, "s1"), _Turno(sesiones, "s2")
    await _entra(b)                 # otra sesión: corre a la vez
    assert not a2.entro.is_set()    # la misma: espera
    await a1.terminar()
    await _entra(a2)
    await a2.terminar()
    await b.terminar()


async def test_el_que_llega_despues_no_adelanta_al_que_espera():
    # A termina con B esperando; C llega entonces y no debe correr a la vez que B.
    sesiones = Sesiones()
    a = _Turno(sesiones, "s")
    await _entra(a)
    b = _Turno(sesiones, "s")
    await asyncio.sleep(0)          # B ya espera en el bloqueo
    await a.terminar()
    c = _Turno(sesiones, "s")
    await _entra(b)
    for _ in range(3):              # C tiene ocasión de entrar si el bloqueo no fuera el de B
        await asyncio.sleep(0)
    assert not c.entro.is_set()
    await b.terminar()
    await _entra(c)
    await c.terminar()
    assert len(sesiones) == 0       # la entrada se borra al terminar el último


async def test_la_entrada_se_borra_al_cancelar():
    sesiones = Sesiones()
    a = _Turno(sesiones, "s")
    await _entra(a)
    b = _Turno(sesiones, "s")
    await asyncio.sleep(0)
    for turno in (b, a):            # primero el que espera, luego el que corre
        turno.tarea.cancel()
    await asyncio.gather(a.tarea, b.tarea, return_exceptions=True)
    assert len(sesiones) == 0
