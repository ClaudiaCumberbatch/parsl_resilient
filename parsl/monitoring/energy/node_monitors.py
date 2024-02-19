import logging
import time
import re
import os

from parsl.monitoring.energy.base import NodeEnergyMonitor, Result
from parsl.monitoring.energy.utils import j_to_uj

logger = logging.getLogger(__name__)

class RaplCPUNodeEnergyMonitor(NodeEnergyMonitor):
    """ Monitor energy using sysfs files created by intel RAPL
    In a large part derived from PyRAPL[https://github.com/powerapi-ng/pyRAPL/tree/master]
    Which is under an MIT License:

    Copyright (c) 2018, INRIA
    Copyright (c) 2018, University of Lille
    
    All rights reserved.
    """
    def __init__(self, debug: bool = True):
        super().__init__(debug=debug)

        self._socket_ids = self.get_socket_ids()
        self._pkg_files = self.get_pkg_files()
        self._dram_files = self.get_dram_files()
        self.prev_reading = None
        self.report()

    def report(self):
        total_energy = 0
        start_time = 0
        end_time = time.clock_gettime(time.CLOCK_MONOTONIC)


        devices = {f"package-{i}": Result(start_time, end_time, -1) for i in self._socket_ids}
        for i in range(len(self._pkg_files)):
            device_file = self._pkg_files[i]
            device_file.seek(0, 0)
            devices[f"package-{self._socket_ids[i]}"].total_energy = float(device_file.readline())
            total_energy += devices[f"package-{self._socket_ids[i]}"].total_energy

            dram_file = self._dram_files[i]
            dram_file.seek(0, 0)
            devices[f"package-{self._socket_ids[i]}"].devices = {"dram": Result(start_time, end_time, float(dram_file.readline()))}

        reading = Result(start_time, end_time, total_energy, devices)

        if self.prev_reading is not None:
            result = reading  - self.prev_reading
            result.total_energy = max(result.total_energy, 0)
        else:
            result = reading

        self.prev_reading = reading
        return result

    def cpu_ids(self) -> list[int]:
        """
        return the cpu id of this machine
        """
        api_file = open('/sys/devices/system/cpu/present', 'r')

        cpu_id_tmp = re.findall('\d+|-', api_file.readline().strip())
        cpu_id_list = []
        for i in range(len(cpu_id_tmp)):
            if cpu_id_tmp[i] == '-':
                for cpu_id in range(int(cpu_id_tmp[i - 1]) + 1, int(cpu_id_tmp[i + 1])):
                    cpu_id_list.append(int(cpu_id))
            else:
                cpu_id_list.append(int(cpu_id_tmp[i]))
        return cpu_id_list


    def get_socket_ids(self) -> list[int]:
        """
        return cpu socket id present on the machine
        """
        socket_id_list = []
        for cpu_id in self.cpu_ids():
            api_file = open('/sys/devices/system/cpu/cpu' + str(cpu_id) + '/topology/physical_package_id')
            socket_id_list.append(int(api_file.readline().strip()))
        return list(set(socket_id_list))

    def _get_socket_directory_names(self) -> list[tuple[str, int]]:
        """
        :return (str, int): directory name, rapl_id
        """

        def add_to_result(directory_info, result):
            """
            check if the directory info could be added to the result list and add it
            """
            dirname, _ = directory_info
            f_name = open(dirname + '/name', 'r')
            pkg_str = f_name.readline()
            if 'package' not in pkg_str:
                return
            package_id = int(pkg_str[:-1].split('-')[1])

            if self._socket_ids is not None and package_id not in self._socket_ids:
                return
            result.append((package_id, ) + directory_info)

        rapl_id = 0
        result_list = []
        while os.path.exists('/sys/class/powercap/intel-rapl/intel-rapl:' + str(rapl_id)):
            dirname = '/sys/class/powercap/intel-rapl/intel-rapl:' + str(rapl_id)
            add_to_result((dirname, rapl_id), result_list)
            rapl_id += 1

        if len(result_list) != len(self._socket_ids):
            raise PyRAPLCantInitDeviceAPI()

        # sort the result list
        result_list.sort(key=lambda t: t[0])
        # return info without socket ids
        return list(map(lambda t: (t[1], t[2]), result_list))

    def get_pkg_files(self):
        directory_name_list = self._get_socket_directory_names()

        rapl_files = []
        for (directory_name, _) in directory_name_list:
            rapl_files.append(open(directory_name + '/energy_uj', 'r'))
        return rapl_files

    def get_dram_files(self):
        directory_name_list = self._get_socket_directory_names()

        def get_dram_file(socket_directory_name, rapl_socket_id, ):
            rapl_device_id = 0
            while os.path.exists(socket_directory_name + '/intel-rapl:' + str(rapl_socket_id) + ':' +
                                 str(rapl_device_id)):
                dirname = socket_directory_name + '/intel-rapl:' + str(rapl_socket_id) + ':' + str(rapl_device_id)
                f_device = open(dirname + '/name', 'r')
                if f_device.readline() == 'dram\n':
                    return open(dirname + '/energy_uj', 'r')
                rapl_device_id += 1
            raise PyRAPLCantInitDeviceAPI()

        rapl_files = []
        for (socket_directory_name, rapl_socket_id) in directory_name_list:
            rapl_files.append(get_dram_file(socket_directory_name, rapl_socket_id))

        return rapl_files


class CrayNodeEnergyMonitor(NodeEnergyMonitor):
    """ Monitor energy using sysfs files created by Cray HSS
    """
    def __init__(self, debug: bool = True):
        super().__init__(debug=debug)
        self._sys_file = open("/sys/cray/pm_counters/energy", "r")
        self.prev_time = 0
        self.prev_energy = 0
        self.report()

    def report(self):
        self._sys_file.seek(0, 0)
        line = self._sys_file.readline()
        energy_j = float(line.partition(" ")[0])
        energy_uj = j_to_uj(energy_j)
        end_time = time.clock_gettime(time.CLOCK_MONOTONIC)

        result = Result(self.prev_time, end_time, energy_uj - self.prev_energy)
        self.prev_time = end_time
        self.prev_energy = energy_uj
        return result

class NVMLGPUEnergyMonitor(NodeEnergyMonitor):
    """ Monitor the energy of NVidia GPUs using Nvidia NVML
    Requires NVML Python library.
    """
    def __init__(self, debug: bool = True):
        super().__init__(debug=debug)

    def report(self):
        pass

class PapiEnergyMonitor(NodeEnergyMonitor):
    """ Monitor energy using the hardware counters provided PAPI. 
    PAPI must be installed on the system, and the desired counters
    must be available. PAPI is a general interface that can used to
    reference a range of counters for both CPU and devices.
    """
    def __init__(self, debug: bool = True):
        super().__init__(debug=debug)

    def report(self):
        pass

class AggregateNodeEnergyMonitor(NodeEnergyMonitor):
    """ Node energy monitor to aggregate energy collection and 
    reporting from several different monitors.
    """
    def __init__(self, debug: bool = True):
        super().__init__(debug=debug)

    def report(self):
        pass

class FakeNodeEnergyMonitor(NodeEnergyMonitor):
    """ Test energy monitor to mimic the behavior of an energy
    monitor without requiring any permissions
    """
    def __init__(self, debug: bool = True):
        super().__init__(debug=debug)
        self.prev_time = time.clock_gettime(time.CLOCK_MONOTONIC)

    def report(self):
        end_time = time.clock_gettime(time.CLOCK_MONOTONIC)
        result = Result(self.prev_time, end_time, 170000)
        self.prev_time = end_time
        return result