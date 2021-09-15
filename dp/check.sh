
gcloud spanner databases execute-sql test-sdk-db --instance=test-instance --sql='Select n.PKey, e.Sortk, n.Ty, e.XF, e.Id, e.Nd, e.LI, e.LF, e.LBl, e.LB, e.LS, e.XBl from Block n join EOP e using (PKey) where n.Pkey = FROM_BASE64("DdQRfBrVQHefL18PXd/d5Q==") and Starts_With(e.Sortk,"A#G#")'
