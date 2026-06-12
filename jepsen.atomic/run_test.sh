unset SSH_AUTH_SOCK
lein run test --username root --password 123456 --nodes-file ./nodes --ssh-private-key /root/.ssh/id_rsa $@
