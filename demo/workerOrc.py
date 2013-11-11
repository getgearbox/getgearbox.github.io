#!/usr/bin/env python

import json
import os
import time
import sys

from gearbox import Worker, ERR_NOT_FOUND, ERR_BAD_REQUEST

def slurp(filename):
    with open(filename, 'r') as f:
        contents = f.read()
    return contents

def write_file(filename, contents):
    with open(filename, 'w') as f:
        f.write(contents)

DBDIR = "/usr/var/gearbox/db/orc/"
class OrcWorker(Worker):
    def __init__(self, cfg):
        super(OrcWorker, self).__init__(cfg)

        self.register_handler( "do_post_orc_server_v1" )
        self.register_handler( "do_get_orc_server_v1" )
        self.register_handler( "do_allocate_nova_instance_v1", self.dummy_handler )
        self.register_handler( "do_update_inventory_asset_v1", self.dummy_handler )
        self.register_handler( "do_validate_inventory_asset_v1", self.dummy_handler )
        self.register_handler( "do_allocate_neutron_port_v1", self.dummy_handler )
        self.register_handler( "do_update_monkier_dnsrecord_v1", self.dummy_handler )
        self.register_handler( "do_create_nova_instance_v1", self.dummy_handler )
        self.register_handler( "do_boot_nova_instance_v1", self.dummy_handler )


    def do_get_orc_server_v1(self, job, resp):
        name = job.resource_name()
        if( os.path.exists(DBDIR + name ) ):
            resp.content( slurp( DBDIR . name ) )
        else:
            raise ERR_NOT_FOUND('server "' + name + '" not found')

        return Worker.WORKER_SUCCESS


    # POST /orc/v1/server
    #
    # {
    #   "server": "foo.isg.apple.com",
    #   "state": "PROVISIONED"
    # }

    def do_post_orc_server_v1(self, job, resp):
        if ( job.operation() == "create" ):
            content = json.loads( job.content() )
            content["state"] = "ALLOCATED"

            content["id"] = job.resource_name()
            write_file(DBDIR + job.resource_name(), json.dumps(content))

            register_job = self.job_manager().job("do_validate_inventory_asset_v1")
            register_job.run()

        else:
            args = job.arguments()
            if ( not os.path.exists( DBDIR + args[0] ) ):
                raise ERR_NOT_FOUND('server "' + args[0] + '" not found')

            content = json.loads( slurp( DBDIR + args[0] ) )
            content['state'] = "PROVISIONING"
            write_file( DBDIR + args[0], json.dumps(content) )

            agents = json.loads( slurp("/etc/gearbox/orc-agents.conf"))
            resp.status().add_message("calling agents")

            run_agents = self.job_manager().job("do_run_global_agents_v1")
            agents_content = {}
            agents_content['agents'] = agents["provision"]
            agents_content["content"] = json.dumps(content)
            run_agents.content( json.dumps(agents_content) )
            r = run_agents.run()
            s = r.status()
            # poll for agents to be done
            while ( not s.has_completed() ):
                time.sleep(1)
                s.sync()

            if ( not s.is_success() ):
                err = getattr( 'gearbox', "ERR_CODE_" + s.code() )
                raise err( s.messages()[-1] )

            content["state"] = "PROVISIONED"
            write_file( DBDIR + args[0], json.dumps(content) )

        return Worker.WORKER_SUCCESS


    def dummy_handler(self, job, resp):
        resp.status().add_message( "Working on job " + job.name() )
        if (job.name() == "do_update_inventory_asset_v1" ):
            resp.status().add_message("Failed connect to inventory system!  Retrying.")
            if ( resp.status().failures() <= 4 ):
                time.sleep(2)
                return Worker.WORKER_RETRY

        time.sleep(10)

        return Worker.WORKER_SUCCESS

if __name__ == "__main__":
    worker = OrcWorker(sys.argv[1])
    worker.run()
