import pymongo
import io

# e.g. userdoc = {'username': "username123", 'groups': ["groupA", "groupB", "groupC"]}
# return group-member mapping from userdocs
def process_userdocs(users):
    groupmembers = {};
    for user in users:
        for groupName in user['groups']:
            if groupName not in groupmembers:
                groupmembers[groupName] = [];
            groupmembers[groupName].append(user['username']);
    return groupmembers

# e.g. groupdoc = {'writable': ["a", "b"], 'readonly': ["a", "b", "c", "d"]}
# e.g. groupmembers = {'groupA': ["a", "b", "c"], 'groupB': ["a", "c"]}
# return string representation of config info
def process_groupdocs(groupdocs, groupmembers):
    configstring = "[gitosis]\nloglevel=DEBUG\n"
    for doc in groupdocs:
        name = doc['name'];
        configstring += "[" + name + "]" + '\n'
        membersstring = "";
        if name not in groupmembers:
            print "group name: " + str(name) + "not found"
        else:
            for member in groupmembers[name]:
                membersstring += member + " ";
        configstring += "members= " + membersstring + '\n';
        writablessstring = "";
        if 'writable' in doc:
            for reponame in doc['writable']:
                writablessstring += reponame + " ";
            configstring += "writable=" + writablessstring + '\n';
        readonlystring = "";
        if 'readonly' in doc:
            for reponame in doc['readonly']:
                readonlystring += reponame;
            configstring += "readonly= " + readonlystring + '\n';
    return configstring;

# Array of objects containing (user, groups) pairs
# e.g. users
# [ 
#   {'username': "username123", 'groups': ["a", "b", "c"]},
#   {'username': "usernameABC", 'groups': ["b", "c", "d"]},
#   ...
# ]
# Array of objects containing repo permissions for each group
# e.g. groups
# [ 
#   {'name': "a", 'writable': ["a", "b", "c"], 'readonly': ["d", "e"]},
#   {'name': "b", 'readonly': ["b", "c", "d"]},
#   ...
# ]
def read_config_from_docs(self, options, cfg, userdocs, groupdocs):
    try:
        # get group-member mapping from userdocs
        groupmembers = process_userdocs(userdocs)
        # get string representation of config info
        configstring = process_groupdocs(groupdocs, groupmembers)
        # convert configstring into configfile
        configfile = io.StringIO(unicode(configstring))
    except (IOError, OSError), e:
        if e.errno == errno.ENOENT:
            # special case this because gitosis-init wants to
            # ignore this particular error case
            raise ConfigFileDoesNotExistError(str(e))
        else:
            raise CannotReadConfigError(str(e))
    try:
        cfg.readfp(configfile)
        print "Authentication completed successfully"
    except (IOError, OSError), e:
        print e
    finally:
        configfile.close()

def read_config_from_mongo(self, options, cfg, mongoDBObject, usersCollectionName, groupsCollectionName):
    db = mongoDBObject;
    users = list(db[usersCollectionName].find())
    validateUserDocs(users);
    groups = list(db[groupsCollectionName].find())
    validateGroupDocs(groups, users);
    read_config_from_docs(self, options, cfg, users, groups);

def validateUserDocs(users):
    if len(users) == 0:
        print "users collection cannot be empty";
        return;
    n_username_error = 0;
    n_groups_error = 0;
    validated_users = [];
    for user in users:
        if 'username' not in user:
            n_username_error += 1;
        if 'groups' not in user:
            n_groups_error += 1;
        if 'groups' in user and 'username' in user:
            validated_users.append(user);
    if n_username_error != 0:
        print str(n_username_error) + "docs did not have username field"
    if n_groups_error != 0:
        print str(n_groups_error) + "docs did not have groups array"
    return validated_users

def validateGroupDocs(groups, users):
    if len(groups) == 0:
        print "groups collection cannot be empty"
        return;
    n_name_error = 0;
    n_wr_error = 0;
    validated_groups = [];
    for group in groups:
        if 'name' not in group:
            n_name_error += 1;
        if 'readonly' not in group and 'writable' not in group:
            n_wr_error += 1;
        if 'name' in group and ('readonly' in group or 'writable' in group):
            validated_groups.append(group);
    if n_name_error != 0:
        print str(n_name_error) + "docs did not have name field"
    if n_wr_error != 0:
        print str(n_wr_error) + "docs did not have readonly or writable array"
    return validated_groups

