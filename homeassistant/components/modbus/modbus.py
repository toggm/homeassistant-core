"""Support for Modbus."""

from __future__ import annotations

import asyncio
from collections import namedtuple
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta
from typing import Any

from pymodbus.client import (
    AsyncModbusSerialClient,
    AsyncModbusTcpClient,
    AsyncModbusUdpClient,
)
from pymodbus.exceptions import ModbusException
from pymodbus.framer import FramerType
from pymodbus.pdu import ModbusPDU
import voluptuous as vol

from homeassistant.components.enocean import (
    DATA_ENOCEAN,
    DOMAIN as ENOCEAN_DOMAIN,
    ENOCEAN_DONGLE,
)
from homeassistant.const import (
    ATTR_STATE,
    CONF_DELAY,
    CONF_HOST,
    CONF_METHOD,
    CONF_NAME,
    CONF_PORT,
    CONF_SCAN_INTERVAL,
    CONF_SLAVE,
    CONF_TIMEOUT,
    CONF_TYPE,
    EVENT_HOMEASSISTANT_STOP,
)
from homeassistant.core import Event, HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.discovery import async_load_platform
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.typing import ConfigType
from homeassistant.util.hass_dict import HassKey

from .const import (
    _LOGGER,
    ATTR_ADDRESS,
    ATTR_HUB,
    ATTR_SLAVE,
    ATTR_UNIT,
    ATTR_VALUE,
    CALL_TYPE_COIL,
    CALL_TYPE_DISCRETE,
    CALL_TYPE_REGISTER_HOLDING,
    CALL_TYPE_REGISTER_INPUT,
    CALL_TYPE_WRITE_COIL,
    CALL_TYPE_WRITE_COILS,
    CALL_TYPE_WRITE_REGISTER,
    CALL_TYPE_WRITE_REGISTERS,
    CONF_BAUDRATE,
    CONF_BYTESIZE,
    CONF_ENOCEAN,
    CONF_ESP_VERSION,
    CONF_INPUT_ADDRESS,
    CONF_MSG_WAIT,
    CONF_OUTPUT_ADDRESS,
    CONF_PARITY,
    CONF_SCAN_GROUPS,
    CONF_SCAN_INTERVAL_MILLIS,
    CONF_STOPBITS,
    DEFAULT_HUB,
    DEVICE_ID,
    MODBUS_DOMAIN as DOMAIN,
    PLATFORMS,
    RTUOVERTCP,
    SERIAL,
    SERVICE_STOP,
    SERVICE_WRITE_COIL,
    SERVICE_WRITE_REGISTER,
    SIGNAL_STOP_ENTITY,
    TCP,
    UDP,
)
from .validators import check_config

DATA_MODBUS_HUBS: HassKey[dict[str, ModbusHub]] = HassKey(DOMAIN)

PRIMARY_RECONNECT_DELAY = 60

ConfEntry = namedtuple("ConfEntry", "call_type attr func_name value_attr_name")  # noqa: PYI024
RunEntry = namedtuple("RunEntry", "attr func value_attr_name")  # noqa: PYI024
PB_CALL = [
    ConfEntry(
        CALL_TYPE_COIL,
        "bits",
        "read_coils",
        "count",
    ),
    ConfEntry(
        CALL_TYPE_DISCRETE,
        "bits",
        "read_discrete_inputs",
        "count",
    ),
    ConfEntry(
        CALL_TYPE_REGISTER_HOLDING,
        "registers",
        "read_holding_registers",
        "count",
    ),
    ConfEntry(
        CALL_TYPE_REGISTER_INPUT,
        "registers",
        "read_input_registers",
        "count",
    ),
    ConfEntry(
        CALL_TYPE_WRITE_COIL,
        "bits",
        "write_coil",
        "value",
    ),
    ConfEntry(
        CALL_TYPE_WRITE_COILS,
        "count",
        "write_coils",
        "values",
    ),
    ConfEntry(
        CALL_TYPE_WRITE_REGISTER,
        "registers",
        "write_register",
        "value",
    ),
    ConfEntry(
        CALL_TYPE_WRITE_REGISTERS,
        "count",
        "write_registers",
        "values",
    ),
]


async def async_modbus_setup(
    hass: HomeAssistant,
    config: ConfigType,
) -> bool:
    """Set up Modbus component."""

    if config[DOMAIN]:
        config[DOMAIN] = check_config(hass, config[DOMAIN])
        if not config[DOMAIN]:
            return False
    if DATA_MODBUS_HUBS in hass.data and config[DOMAIN] == []:
        hubs = hass.data[DATA_MODBUS_HUBS]
        for hub in hubs.values():
            if not await hub.async_setup():
                return False
        hub_collect = hass.data[DATA_MODBUS_HUBS]
    else:
        hass.data[DATA_MODBUS_HUBS] = hub_collect = {}

    for conf_hub in config[DOMAIN]:
        my_hub = ModbusHub(hass, conf_hub)
        hub_collect[conf_hub[CONF_NAME]] = my_hub

        # modbus needs to be activated before components are loaded
        # to avoid a racing problem
        if not await my_hub.async_setup():
            return False

        # Register modbus enocean dongle
        if conf_hub.get(CONF_ENOCEAN):
            await my_hub.async_create_and_register_enocean_dongle(
                conf_hub[CONF_ENOCEAN]
            )

        # load platforms
        for component, conf_key in PLATFORMS:
            if conf_key in conf_hub:
                hass.async_create_task(
                    async_load_platform(hass, component, DOMAIN, conf_hub, config)
                )

    async def async_stop_modbus(event: Event) -> None:
        """Stop Modbus service."""
        for client in hub_collect.values():
            await client.async_close()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, async_stop_modbus)

    async def async_write_register(service: ServiceCall) -> None:
        """Write Modbus registers."""
        slave = 1
        if ATTR_UNIT in service.data:
            slave = int(float(service.data[ATTR_UNIT]))

        if ATTR_SLAVE in service.data:
            slave = int(float(service.data[ATTR_SLAVE]))
        address = int(float(service.data[ATTR_ADDRESS]))
        value = service.data[ATTR_VALUE]
        hub = hub_collect[service.data.get(ATTR_HUB, DEFAULT_HUB)]
        if isinstance(value, list):
            await hub.async_pb_call(
                slave,
                address,
                [int(float(i)) for i in value],
                CALL_TYPE_WRITE_REGISTERS,
            )
        else:
            await hub.async_pb_call(
                slave, address, int(float(value)), CALL_TYPE_WRITE_REGISTER
            )

    async def async_write_coil(service: ServiceCall) -> None:
        """Write Modbus coil."""
        slave = 1
        if ATTR_UNIT in service.data:
            slave = int(float(service.data[ATTR_UNIT]))
        if ATTR_SLAVE in service.data:
            slave = int(float(service.data[ATTR_SLAVE]))
        address = service.data[ATTR_ADDRESS]
        state = service.data[ATTR_STATE]
        hub = hub_collect[service.data.get(ATTR_HUB, DEFAULT_HUB)]
        if isinstance(state, list):
            await hub.async_pb_call(slave, address, state, CALL_TYPE_WRITE_COILS)
        else:
            await hub.async_pb_call(slave, address, state, CALL_TYPE_WRITE_COIL)

    for x_write in (
        (SERVICE_WRITE_REGISTER, async_write_register, ATTR_VALUE, cv.positive_int),
        (SERVICE_WRITE_COIL, async_write_coil, ATTR_STATE, cv.boolean),
    ):
        hass.services.async_register(
            DOMAIN,
            x_write[0],
            x_write[1],
            schema=vol.Schema(
                {
                    vol.Optional(ATTR_HUB, default=DEFAULT_HUB): cv.string,
                    vol.Exclusive(ATTR_SLAVE, "unit"): cv.positive_int,
                    vol.Exclusive(ATTR_UNIT, "unit"): cv.positive_int,
                    vol.Required(ATTR_ADDRESS): cv.positive_int,
                    vol.Required(x_write[2]): vol.Any(
                        cv.positive_int, vol.All(cv.ensure_list, [x_write[3]])
                    ),
                }
            ),
        )

    async def async_stop_hub(service: ServiceCall) -> None:
        """Stop Modbus hub."""
        async_dispatcher_send(hass, SIGNAL_STOP_ENTITY)
        hub = hub_collect[service.data[ATTR_HUB]]
        await hub.async_close()

    hass.services.async_register(
        DOMAIN,
        SERVICE_STOP,
        async_stop_hub,
        schema=vol.Schema({vol.Required(ATTR_HUB): cv.string}),
    )
    return True


class ModbusUpdateListener:
    """Update listener configuration."""

    def __init__(
        self,
        slave: int,
        input_type: str,
        min_address: int,
        max_address: int,
        func: Callable[[ModbusPDU | None, int, str, int], Coroutine[Any, Any, None]],
    ) -> None:
        """Initialize the Modbus update listener configuration."""
        self._slave = slave
        self._input_type = input_type
        self._min_address = min_address
        self._max_address = max_address
        self._func = func

    def get_min_address(self) -> int:
        """Get min address."""
        return self._min_address

    def get_max_address(self) -> int:
        """Get max address."""
        return self._max_address

    def notify(
        self, result: ModbusPDU | None, offset: int
    ) -> Coroutine[Any, Any, None]:
        """Notify update listener."""
        return self._func(
            result, self._slave, self._input_type, self._min_address - offset
        )


class ModbusHub:
    """Thread safe wrapper class for pymodbus."""

    def __init__(self, hass: HomeAssistant, client_config: dict[str, Any]) -> None:
        """Initialize the Modbus hub."""

        # generic configuration
        self._client: (
            AsyncModbusSerialClient | AsyncModbusTcpClient | AsyncModbusUdpClient | None
        ) = None
        self._lock = asyncio.Lock()
        self.event_connected = asyncio.Event()
        self.hass = hass
        self.name = client_config[CONF_NAME]
        self._config_type = client_config[CONF_TYPE]
        self.config_delay = client_config[CONF_DELAY]
        self._pb_request: dict[str, RunEntry] = {}
        self._connect_task: asyncio.Task
        self._last_log_error: str = ""
        self._scan_interval = int(client_config[CONF_SCAN_INTERVAL])
        self._pb_class = {
            SERIAL: AsyncModbusSerialClient,
            TCP: AsyncModbusTcpClient,
            UDP: AsyncModbusUdpClient,
            RTUOVERTCP: AsyncModbusTcpClient,
        }
        self._pb_params = {
            "port": client_config[CONF_PORT],
            "timeout": client_config[CONF_TIMEOUT],
            "retries": 3,
        }
        if self._config_type == SERIAL:
            # serial configuration
            if client_config[CONF_METHOD] == "ascii":
                self._pb_params["framer"] = FramerType.ASCII
            else:
                self._pb_params["framer"] = FramerType.RTU
            self._pb_params.update(
                {
                    "baudrate": client_config[CONF_BAUDRATE],
                    "stopbits": client_config[CONF_STOPBITS],
                    "bytesize": client_config[CONF_BYTESIZE],
                    "parity": client_config[CONF_PARITY],
                }
            )
        else:
            # network configuration
            self._pb_params["host"] = client_config[CONF_HOST]
            if self._config_type == RTUOVERTCP:
                self._pb_params["framer"] = FramerType.RTU
            else:
                self._pb_params["framer"] = FramerType.SOCKET
        self._update_listeners_by_scan_group = dict[
            str, dict[Any, list[ModbusUpdateListener]]
        ]()
        self._scan_groups = dict[str, int]()
        for entry in client_config[CONF_SCAN_GROUPS]:
            name = entry[CONF_NAME]
            self._scan_groups[name] = int(entry[CONF_SCAN_INTERVAL_MILLIS])
            self._update_listeners_by_scan_group[name] = {}

        if CONF_MSG_WAIT in client_config:
            self._msg_wait = client_config[CONF_MSG_WAIT] / 1000
        elif self._config_type == SERIAL:
            self._msg_wait = 30 / 1000
        else:
            self._msg_wait = 0

    def _log_error(self, text: str) -> None:
        if text == self._last_log_error:
            return
        self._last_log_error = text
        log_text = f"Pymodbus: {self.name}: {text}"
        _LOGGER.error(log_text)

    async def async_pb_connect(self) -> None:
        """Connect to device, async."""
        while True:
            try:
                if await self._client.connect():  # type: ignore[union-attr]
                    _LOGGER.info(f"modbus {self.name} communication open")
                    break
            except ModbusException as exception_error:
                self._log_error(
                    f"{self.name} connect failed, please check your configuration ({exception_error!s})"
                )
            _LOGGER.info(
                f"modbus {self.name} connect NOT a success ! retrying in {PRIMARY_RECONNECT_DELAY} seconds"
            )
            await asyncio.sleep(PRIMARY_RECONNECT_DELAY)

        if self.config_delay:
            await asyncio.sleep(self.config_delay)
        self.config_delay = 0
        self.event_connected.set()

    async def async_setup(self) -> bool:
        """Set up pymodbus client."""
        try:
            self._client = self._pb_class[self._config_type](**self._pb_params)
        except ModbusException as exception_error:
            self._log_error(str(exception_error))
            return False

        for entry in PB_CALL:
            func = getattr(self._client, entry.func_name)
            self._pb_request[entry.call_type] = RunEntry(
                entry.attr, func, entry.value_attr_name
            )

        self._connect_task = self.hass.async_create_background_task(
            self.async_pb_connect(), "modbus-connect"
        )
        self.start_update_listener()
        return True

    async def async_create_and_register_enocean_dongle(
        self, config: dict[str, Any]
    ) -> None:
        """Create and register enocean dongle."""
        # pylint: disable=import-outside-toplevel
        from .modbusenoceandongle import ModbusEnOceanDongle
        from .modbusenoceanwago750adapter import ModbusEnOceanWago750Adapter
        # pylint: enable=import-outside-toplevel

        input_address = config[CONF_INPUT_ADDRESS]
        output_address = config[CONF_OUTPUT_ADDRESS]
        slave = config[CONF_SLAVE]
        esp_version = config.get(CONF_ESP_VERSION, 3)
        # Change as soon as other modbus enocean adapters are supported
        adapter = ModbusEnOceanWago750Adapter(
            self, slave, input_address, output_address
        )
        dongle = ModbusEnOceanDongle(self.hass, adapter, esp_version)
        # Register dongle if not another enocean dongle was registered yet
        if self.hass.config_entries.async_entries(ENOCEAN_DOMAIN):
            _LOGGER.debug("Register modbus enocean dongle")
            enocean_data = self.hass.data.setdefault(DATA_ENOCEAN, {})
            await dongle.async_setup()
            enocean_data[ENOCEAN_DONGLE] = dongle

    def start_update_listener(self) -> None:
        """Possibly start monitoring of updates."""
        for scan_group, interval_millis in self._scan_groups.items():
            _LOGGER.debug(
                "Register scan listener scan_group=%s, interval_millis=%s",
                scan_group,
                interval_millis,
            )
            async_track_time_interval(
                self.hass,
                self.async_update_function(scan_group),
                timedelta(milliseconds=interval_millis),
            )

    def register_update_listener(
        self,
        scan_group: str,
        slave: int,
        input_type: str,
        min_address: int,
        max_address: int,
        func: Callable[[ModbusPDU | None, int, str, int], Coroutine[Any, Any, None]],
    ) -> None:
        """Register update listener."""
        _LOGGER.debug(
            "Register update listener slave=%s, input_type=%s, min_address=%s, max_address=%s in scan_group=%s",
            slave,
            input_type,
            min_address,
            max_address,
            scan_group,
        )
        update_listeners = self._update_listeners_by_scan_group[scan_group]
        key = (slave, input_type)
        if key in update_listeners:
            update_listeners[key].append(
                ModbusUpdateListener(slave, input_type, min_address, max_address, func)
            )
        else:
            update_listeners[key] = [
                ModbusUpdateListener(slave, input_type, min_address, max_address, func)
            ]

    def async_update_function(
        self, scan_group: str
    ) -> Callable[[datetime], Coroutine[Any, Any, None] | None]:
        """Return async update function per scan group."""

        async def async_update(now: datetime | None = None) -> None:
            """Update the state of all entities in a given scan group."""
            # remark "now" is a dummy parameter to avoid problems with
            # async_track_time_interval
            for (
                (slave, input_type),
                listeners,
            ) in self._update_listeners_by_scan_group[scan_group].items():
                min_address = 1000
                max_address = 0
                for listener in listeners:
                    min_address = min(min_address, listener.get_min_address())
                    max_address = max(max_address, listener.get_max_address())
                _LOGGER.debug(
                    "query modbus: scan_group=%s, slave=%s, minAdress=%s, maxAdress=%s, input_type=%s",
                    scan_group,
                    slave,
                    min_address,
                    max_address,
                    input_type,
                )
                result = await self.async_pb_call(
                    slave, min_address, max_address + 1 - min_address, input_type
                )
                for listener in listeners:
                    await listener.notify(result=result, offset=min_address)

        return async_update

    async def async_restart(self) -> None:
        """Reconnect client."""
        if self._client:
            await self.async_close()

        await self.async_setup()

    async def async_close(self) -> None:
        """Disconnect client."""
        self.event_connected.set()
        if not self._connect_task.done():
            self._connect_task.cancel()

        async with self._lock:
            if self._client:
                try:
                    self._client.close()
                except ModbusException as exception_error:
                    self._log_error(str(exception_error))
                del self._client
                self._client = None
                message = f"modbus {self.name} communication closed"
                _LOGGER.info(message)

    async def low_level_pb_call(
        self, slave: int | None, address: int, value: int | list[int], use_call: str
    ) -> ModbusPDU | None:
        """Call sync. pymodbus."""
        kwargs: dict[str, Any] = (
            {DEVICE_ID: slave} if slave is not None else {DEVICE_ID: 1}
        )
        entry = self._pb_request[use_call]

        if use_call in {"write_registers", "write_coils"}:
            if not isinstance(value, list):
                value = [value]

        kwargs[entry.value_attr_name] = value
        try:
            result: ModbusPDU = await entry.func(address, **kwargs)
        except ModbusException as exception_error:
            error = f"Error: device: {slave} address: {address} -> {exception_error!s}"
            self._log_error(error)
            return None
        if not result:
            error = (
                f"Error: device: {slave} address: {address} -> pymodbus returned None"
            )
            self._log_error(error)
            return None
        if not hasattr(result, entry.attr):
            error = f"Error: device: {slave} address: {address} -> {result!s}"
            self._log_error(error)
            return None
        if result.isError():
            error = f"Error: device: {slave} address: {address} -> pymodbus returned isError True"
            self._log_error(error)
            return None
        return result

    async def async_pb_call(
        self,
        unit: int | None,
        address: int,
        value: int | list[int],
        use_call: str,
    ) -> ModbusPDU | None:
        """Convert async to sync pymodbus call."""
        async with self._lock:
            if not self._client:
                return None
            result = await self.low_level_pb_call(unit, address, value, use_call)
            if self._msg_wait:
                # small delay until next request/response
                await asyncio.sleep(self._msg_wait)
            return result
