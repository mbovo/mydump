#!/usr/bin/env python
DOCUMENTATION = """
---
module: mytest
version_added: 0.1
short_description: MY test
options:
	username:
		description:
			- the username

"""

def main():
	module = AnsibleModule( argument_spec = dict (username=dict(required=True)))
	username = module.params.get('username')
	module.exit_json(changed=True, msg=username)

from ansible.module_utils.basic import *
main()
