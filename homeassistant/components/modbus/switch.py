"""Support for Modbus switches."""

from __future__ import annotations

import logging
from typing import Any

from pymodbus.pdu import ModbusPDU

from homeassistant.components.switch import ENTITY_ID_FORMAT, SwitchEntity
from homeassistant.const import CONF_NAME, CONF_SWITCHES
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

from . import get_hub
from .entity import ModbusToggleEntity
from .const import CALL_TYPE_COIL, CALL_TYPE_DISCRETE
from .modbus import ModbusHub

PARALLEL_UPDATES = 1
_LOGGER = logging.getLogger(__name__)


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Read configuration and create Modbus switches."""
    if discovery_info is None or not (switches := discovery_info[CONF_SWITCHES]):
        return
    hub = get_hub(hass, discovery_info[CONF_NAME])
    async_add_entities(ModbusSwitch(hass, hub, config) for config in switches)


class ModbusSwitch(ModbusToggleEntity, SwitchEntity):
    """Base class representing a Modbus switch."""

    def __init__(
        self,
        hass: HomeAssistant,
        hub: ModbusHub,
        config: dict[str, Any],
    ) -> None:
        """Initialize the modbus switch."""
        super().__init__(hass, hub, config)
        self.entity_id = ENTITY_ID_FORMAT.format(self._id)
        self._result = 0

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Set switch on."""
        await self.async_turn(self.command_on)

    async def async_update_from_result(
        self,
        raw_result: ModbusPDU | None,
        slave_id: int,
        input_type: str,
        address: int,
    ) -> None:
        """Update the state of the switch."""
        if raw_result is None:
            self._attr_available = False
            self._result = 0
            self.async_write_ha_state()
        else:
            self._attr_available = True
            if input_type in (CALL_TYPE_COIL, CALL_TYPE_DISCRETE):
                self._result = raw_result.bits[address]
            else:
                self._result = raw_result.registers[address]
            new_value = bool(self._result & 1)

            if new_value != self._attr_is_on:
                _LOGGER.debug(
                    "change switch state: input_type=%s, address=%s, new_value=%s, _attr_is_on=%s",
                    input_type,
                    address,
                    new_value,
                    self._attr_is_on,
                )

                self._attr_is_on = new_value
                self.async_write_ha_state()
