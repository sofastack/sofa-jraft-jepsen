unset SSH_AUTH_SOCK
lein run test --username root --password xxx --nodes-file  ./nodes  --ssh-private-key /xxx/.ssh/id_rsa $@
