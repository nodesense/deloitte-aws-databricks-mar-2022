https://docs.databricks.com/administration-guide/cloud-configurations/aws/vpc-peering.html

NoteBook, try to run

DNS Check

```
%sh host -t a redshift-cluster-1.ch3jfahbfyls.us-east-2.redshift.amazonaws.com
```

Port Firewall check

```
%sh nc -zv redshift-cluster-1.ch3jfahbfyls.us-east-2.redshift.amazonaws.com 5439
```


JDBC

```
```
