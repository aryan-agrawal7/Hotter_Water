Instructions to run:
1) compile using "./run.sh"
2) (If Mac as it has some firewall blocks connections)
    do "./setup_firewall.sh" 
    to allow incoming packets on executable programs compiled above.
3) NamedServer: ./NamedServer
    -- prints port and ip (lo0 and en0)
4) StorageServer: ./StorageServer <NameServer_ip> <NameServer_port>
5) Client: ./Client <NameServer_ip> <NameServer_port>
    (Generally use the en0 ip printed in NamedServer as <NameServer_ip> above).

Assumptions:
1. Our files are 1 indexed not 0 indexed (both sentences and words).
2. The write command is an atomic command and hence only when the full command is over (On detection of ETIRW), then we apply the change to the file. Until then the file is unchanged.
3. Each file is in 1 SS only.
4. Only with read permissions you can view additional INFO like timestamp of access and Size. Hence INFO command has same permissions as READ.
5. Filenames are unique.
6. Usernames are unique.

Client can do these:
    VIEW | VIEW -a | VIEW -l | VIEW -al
    READ <filename>
    CREATE <filename>
    WRITE <filename> <sentence_number>   
        (followed by many lines as "<word_index> <content>",
         ends with "ETIRW")
    UNDO <filename>
    INFO <filename>
    DELETE <filename>
    STREAM <filename>
    LIST
    ADDACCESS -R <filename> <username>
    ADDACCESS -W <filename> <username>
    REMACCESS  <filename> <username>
    EXEC <filename>

Bonus(attempted 20/50 marks):
    ii. 
    CHECKPOINT <filename> <checkpoint_tag> -- Creates a checkpoint with the given tag
    VIEWCHECKPOINT <filename> <checkpoint_tag> -- Views the content of the specified checkpoint
    REVERT <filename> <checkpoint_tag> -- Reverts the file to the specified checkpoint
    LISTCHECKPOINTS <filename> -- Lists all checkpoints for the specified file
    iii.
    REQUESTACCESS -R/-W <filename>
    VIEWREQUESTS
    APPROVEACCESS <filename> <username>
    DENYACCESS <filename> <username>

LLM Chat:
1. https://docs.google.com/document/d/1WpTqGaegzMP7KWb9Xj5GXvX3Lz83caHanauxEl7_Ves/edit?usp=sharing
2. https://docs.google.com/document/d/1XDW-OG8k8m_uET1oagDZGfHlwtMpRlQD7gz-hi86l4w/edit?usp=sharing