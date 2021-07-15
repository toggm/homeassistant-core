"""Representation of an EnOcean dongle based on a modbus hub."""

from collections.abc import Callable
from typing import Any

from enocean.protocol.packet import RadioPacket

from homeassistant.components.enocean.const import (  # pylint: disable=hass-component-root-import
    SIGNAL_RECEIVE_MESSAGE,
    SIGNAL_SEND_MESSAGE,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect, dispatcher_send

from .const import _LOGGER
from .modbusenoceanadapter import ModbusEnoceanAdapter
from .modbusenoceancommunicator import ModbusEnoceanCommunicator


class ModbusEnOceanDongle:
    """Representation of an EnOcean dongle based on a modbus hub.

    The dongle is responsible for receiving the ENOcean frames,
    creating devices if needed, and dispatching messages to platforms.
    """

    def __init__(
        self, hass: HomeAssistant, adapter: ModbusEnoceanAdapter, esp_version: int
    ) -> None:
        """Initialize the EnOcean dongle."""

        self._communicator = ModbusEnoceanCommunicator(
            hass=hass, adapter=adapter, esp_version=esp_version, callback=self.callback
        )
        self.adapter = adapter
        self.identifier = adapter.identifier
        self.hass = hass
        self.dispatcher_disconnect_handle: Callable[[], None] | None = None

    async def async_setup(self) -> None:
        """Finish the setup of the bridge and supported platforms."""
        self._communicator.start()
        self.dispatcher_disconnect_handle = async_dispatcher_connect(
            self.hass, SIGNAL_SEND_MESSAGE, self._send_message_callback
        )

    def unload(self) -> None:
        """Disconnect callbacks established at init time."""
        _LOGGER.debug("unload modbus encoean dongle")
        if self.dispatcher_disconnect_handle:
            self.dispatcher_disconnect_handle()
            self.dispatcher_disconnect_handle = None

    @callback
    def _send_message_callback(self, command: Any) -> None:
        """Send a command through the EnOcean dongle."""
        self._communicator.send(command)

    @callback
    def callback(self, packet: Any) -> None:
        """Handle EnOcean device's callback.

        This is the callback function called by python-enocean whenever there
        is an incoming packet.
        """

        if isinstance(packet, RadioPacket):
            _LOGGER.debug("Received radio packet: %s", packet)
            dispatcher_send(self.hass, SIGNAL_RECEIVE_MESSAGE, packet)
