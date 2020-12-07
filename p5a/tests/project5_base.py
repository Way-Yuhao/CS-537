import os, subprocess, shutil

import toolspath
from testing import Test, BuildTest

call_fs = "xcheck "

def readall(filename):
  f = open(filename, 'r')
  s = f.read()
  f.close()
  return s

class FSBuildTest(BuildTest):
  targets = ['xfsck']

  def run(self):
    self.clean(['xfsck', '*.o'])
    if not self.make(self.targets, required=False):
      self.run_util(['gcc', 'xfsck.c','-iquote/u/c/s/cs537-1/xv6-sp20/include' , '-o', 'xfsck'])
    self.done()

class FSTest(Test):
  def run(self, command = None, stdout = None, stderr = None,
          addl_args = []):
    in_path = self.test_path + '/images/' + self.name
    if command == None:
      command = ['./xfsck', in_path]

    out_path = self.test_path + '/out/' + self.name
    err_path = self.test_path + '/err/' + self.name
    self.command = call_fs + in_path + \
        "\n and check out the test folder\n " + self.test_path \
        + '/err/' + self.name + \
        "\n to compare your error output with reference outputs. "

    if stdout == None:
      stdout = readall(out_path)

    if stderr == None:
      stderr = readall(err_path)

    self.runexe(command, status=self.status, stderr=stderr, stdout=stdout)
    self.done()

######################### Built-in Commands #########################

class Good(FSTest):
  name = 'Good'
  description = 'run on a good filesystem'
  timeout = 10
  status = 0
  point_value = 2

class SuperBlock(FSTest):
  name = 'Superblock'
  description = 'run on a filesystem with corrupted superblock'
  timeout = 10
  status = 1
  point_value = 2

class Nonexistant(FSTest):
  name = 'Nonexistant'
  description = 'run on a nonexistant filesystem'
  timeout = 10
  status = 1
  point_value = 2

class Badinode(FSTest):
  name = 'Badinode'
  description = 'run on a filesystem with a bad type in an inode'
  timeout = 10
  status = 1
  point_value = 5

class BadSize(FSTest):
  name = 'Badsize'
  description = 'run on a filesystem with a bad file size in an inode'
  timeout = 10
  status = 1
  point_value = 5

class Badaddr(FSTest):
  name = 'Badaddr'
  description = 'run on a filesystem with a bad direct address in an inode'
  timeout = 10
  status = 1
  point_value = 5

class Badindir1(FSTest):
  name = 'Badindir1'
  description = 'run on a filesystem with a bad indirect address in an inode'
  timeout = 10
  status = 1
  point_value = 5

class Badindir2(FSTest):
  name = 'Badindir2'
  description = 'run on a filesystem with a bad indirect address in an inode'
  timeout = 10
  status = 1
  point_value = 5

class Badroot(FSTest):
  name = 'Badroot'
  description = 'run on a filesystem with a root directory in bad location'
  timeout = 10
  status = 1
  point_value = 2

class Badroot2(FSTest):
  name = 'Badroot2'
  description = 'run on a filesystem with a bad root directory in good location'
  timeout = 10
  status = 1
  point_value = 5

class Badfmt(FSTest):
  name = 'Badfmt'
  description = 'run on a filesystem without . or .. directories'
  timeout = 10
  status = 1
  point_value = 5


class Mrkfree(FSTest):
  name = 'Mrkfree'
  description = 'run on a filesystem with an inuse direct block marked free'
  timeout = 10
  status = 1
  point_value = 5

class Mrkfreeindir(FSTest):
  name = 'Indirfree'
  description = 'run on a filesystem with an inuse indirect block marked free'
  timeout = 10
  status = 1
  point_value = 5

class Mrkused(FSTest):
  name = 'Mrkused'
  description = 'run on a filesystem with a free block marked used'
  timeout = 10
  status = 1
  point_value = 5

class Addronce(FSTest):
  name = 'Addronce'
  description = 'run on a filesystem with an address used more than once'
  timeout = 10
  status = 1
  point_value = 5

class Imrkused(FSTest):
  name = 'Imrkused'
  description = 'run with inode marked used, but not referenced in a directory'
  timeout = 10
  status = 1
  point_value = 5

class Imrkfree(FSTest):
  name = 'Imrkfree'
  description = 'run with inode marked free, but referenced in a directory'
  timeout = 10
  status = 1
  point_value = 5

class Badrefcnt(FSTest):
  name = 'Badrefcnt'
  description = 'run on fs which has an inode with a bad reference count'
  timeout = 10
  status = 1
  point_value = 5

class Goodrefcnt(FSTest):
  name = 'Goodrefcnt'
  description = 'run on fs with only good reference counts'
  timeout = 10
  status = 0
  point_value = 2

class Dironce(FSTest):
  name = 'Dironce'
  description = 'run on fs with a directory appearing more than once'
  timeout = 10
  status = 1
  point_value = 5

class Goodrm(FSTest):
  name = 'Goodrm'
  description = 'run on good fs with a file removed and a new directory created'
  timeout = 10
  status = 0
  point_value = 3

class Goodrm2(FSTest):
  name = 'Goodrm2'
  description = 'run on good fs with a new directory created and then a file removed'
  timeout = 10
  status = 0
  point_value = 3

class Goodlink(FSTest):
  name = 'Goodlink'
  description = 'run on good fs with some hard links and a linked file removed'
  timeout = 10
  status = 0
  point_value = 3

class Goodlarge(FSTest):
  name = 'Goodlarge'
  description = 'run on large good fs'
  timeout = 10
  status = 0
  point_value = 3

class Mismatch(FSTest):
  name = 'Mismatch'
  description = '**extra** run on a filesystem with .. pointing to the wrong directory'
  timeout = 5
  status = 1
  point_value = 7

class Loop(FSTest):
  name = 'Loop'
  description = '**extra**  run on a filesystem with a loop in directory tree'
  timeout = 5
  status = 1
  point_value = 8

class Repair(FSTest):
  name = 'Repair'
  description = '**extra**  repair a filesystem with lost inodes. check if every lost inode is found'
  timeout = 10
  status = 0
  point_value = 10

  def run(self, command = None, stdout = None, stderr = None,
          addl_args = []):
    shutil.copyfile(self.test_path + '/images/' + self.name,
                    self.project_path + "/" + self.name)
    in_path = self.project_path + "/" + self.name
    if command == None:
      command = ['./xcheck', '-r', in_path]

    self.runexe(command)

    out1 = subprocess.Popen([self.test_path + '/util/repair_check_easy', in_path],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]
    if 'PASSED' not in out1: self.fail("image is not correctly repaired.")
    self.done()

class Repair2(FSTest):
  name = 'Repair2'
  description = '**extra**  repair a filesystem with lost inodes. check if there are any inconsistencies in the repaired image.'
  timeout = 10
  status = 0
  point_value = 5

  def run(self, command = None, stdout = None, stderr = None,
          addl_args = []):
    shutil.copyfile(self.test_path + '/images/Repair',
                    self.project_path + "/" + self.name)
    in_path = self.project_path + "/" + self.name
    if command == None:
      command = ['./xcheck', '-r', in_path]

    self.runexe(command)

    out1 = subprocess.Popen([self.test_path + '/util/repair_check', in_path],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0]
    if 'PASSED' not in out1: self.fail("image is not correctly repaired.")
    self.done()




#=========================================================================

all_tests = [
  # Easy tests
  Good,
  Goodrefcnt,
  Nonexistant,
  SuperBlock,

  # Medium/hard tests
  BadSize,
  Badinode,
  Badaddr,
  Badindir1,
  Badfmt,
  Mrkfree,
  Mrkfreeindir,
  Mrkused,
  Addronce,
  Imrkused,
  Imrkfree,
  Badrefcnt,
  Badindir2,
  Dironce,
  Goodrm,
  Goodrm2,
  Goodlink,
  Goodlarge,
  ]

build_test = FSBuildTest

from testing.runtests import main
main(build_test, all_tests)
