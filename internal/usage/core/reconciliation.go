package core

type ProviderReconciliation struct {
	Provider              string `json:"provider"`
	GatewaySpendMicros    int64  `json:"gateway_spend_micros"`
	OrgSpendMicros        int64  `json:"org_spend_micros"`
	OverlapMicros         int64  `json:"overlap_micros"`
	DiscrepancyMicros     int64  `json:"discrepancy_micros"`
	ReconciledSpendMicros int64  `json:"reconciled_spend_micros"`
}

type ReconciliationReport struct {
	ByProvider             map[string]ProviderReconciliation `json:"by_provider"`
	TotalGatewayMicros     int64                             `json:"total_gateway_micros"`
	TotalOrgMicros         int64                             `json:"total_org_micros"`
	TotalOverlapMicros     int64                             `json:"total_overlap_micros"`
	TotalDiscrepancyMicros int64                             `json:"total_discrepancy_micros"`
	TotalReconciledMicros  int64                             `json:"total_reconciled_micros"`
}

func ReconcileProvider(provider string, gatewaySpend, orgSpend int64) ProviderReconciliation {
	rec := ProviderReconciliation{
		Provider:           provider,
		GatewaySpendMicros: gatewaySpend,
		OrgSpendMicros:     orgSpend,
	}

	switch {
	case gatewaySpend == 0:
		rec.ReconciledSpendMicros = orgSpend
	case orgSpend == 0:
		rec.ReconciledSpendMicros = gatewaySpend
	default:
		if gatewaySpend < orgSpend {
			rec.OverlapMicros = gatewaySpend
			rec.DiscrepancyMicros = orgSpend - gatewaySpend
			rec.ReconciledSpendMicros = orgSpend
		} else {
			rec.OverlapMicros = orgSpend
			rec.DiscrepancyMicros = gatewaySpend - orgSpend
			rec.ReconciledSpendMicros = gatewaySpend
		}
	}

	return rec
}

func ReconcileAll(gatewayByProvider, orgByProvider map[string]int64) ReconciliationReport {
	allProviders := make(map[string]bool)
	for p := range gatewayByProvider {
		allProviders[p] = true
	}
	for p := range orgByProvider {
		allProviders[p] = true
	}

	report := ReconciliationReport{
		ByProvider: make(map[string]ProviderReconciliation),
	}

	for p := range allProviders {
		gw := gatewayByProvider[p]
		org := orgByProvider[p]
		rec := ReconcileProvider(p, gw, org)
		report.ByProvider[p] = rec

		report.TotalGatewayMicros += rec.GatewaySpendMicros
		report.TotalOrgMicros += rec.OrgSpendMicros
		report.TotalOverlapMicros += rec.OverlapMicros
		report.TotalDiscrepancyMicros += rec.DiscrepancyMicros
		report.TotalReconciledMicros += rec.ReconciledSpendMicros
	}

	return report
}
