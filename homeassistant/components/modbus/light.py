"""Support for Modbus lights."""

from __future__ import annotations

import logging
from typing import Any

from pymodbus.pdu import ModbusPDU

from homeassistant.components.light import ENTITY_ID_FORMAT, ColorMode, LightEntity
from homeassistant.const import CONF_LIGHTS, CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

from . import get_hub
from .const import CALL_TYPE_COIL, CALL_TYPE_DISCRETE
from .entity import BaseSwitch
from .modbus import ModbusHub

PARALLEL_UPDATES = 1
_LOGGER = logging.getLogger(__name__)


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Read configuration and create Modbus lights."""
    if discovery_info is None or not (lights := discovery_info[CONF_LIGHTS]):
        return
    hub = get_hub(hass, discovery_info[CONF_NAME])
    async_add_entities(ModbusLight(hass, hub, config) for config in lights)


class ModbusLight(BaseSwitch, LightEntity):
    """Class representing a Modbus light."""

    _attr_color_mode = ColorMode.ONOFF
    _attr_supported_color_modes = {ColorMode.ONOFF}

    def __init__(
        self,
        hass: HomeAssistant,
        hub: ModbusHub,
        config: dict[str, Any],
    ) -> None:
        """Initialize the modbus light."""
        super().__init__(hass, hub, config)
        self.entity_id = ENTITY_ID_FORMAT.format(self._id)
        self._result = 0

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Set light on."""
        await self.async_turn(self.command_on)

    async def async_update_from_result(
        self,
        raw_result: ModbusPDU | None,
        slave_id: int,
        input_type: str,
        address: int,
    ) -> None:
        """Update the state of the light."""
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
                    "change light state: input_type=%s, address=%s, new_value=%s, _attr_is_on=%s",
                    input_type,
                    address,
                    new_value,
                    self._attr_is_on,
                )

                self._attr_is_on = new_value
                self.async_write_ha_state()
