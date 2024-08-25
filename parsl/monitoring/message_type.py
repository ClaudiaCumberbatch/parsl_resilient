from enum import Enum


class MessageType(Enum):

    # Reports any task related info such as launch, completion etc.
    TASK_INFO = 0

    # Reports of resource utilization on a per-task basis
    # per process (worker)
    RESOURCE_INFO = 1

    # Top level workflow information
    WORKFLOW_INFO = 2

    # Reports of the resource capacity for each node
    # per node (manager, same hostname)
    NODE_INFO = 3

    # Reports of the block info
    BLOCK_INFO = 4

    # Reports of node-level energy information
    ENERGY_INFO = 5

    # Reports of failure information
    FAILURE_INFO = 6

    # Reports of Executor
    EXECUTOR_INFO = 7
