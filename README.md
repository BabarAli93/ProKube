# ProKube

Deep Neural Network (DNN) and Machine Learning (ML) models/ inferences produce highly accurate results demanding
enormous computational resources. The limited capacity of end-user smart gadgets drives companies to exploit computational
resources in an edge-to-cloud continuum and host applications at user-facing locations with users requiring fast responses. Kubernetes
hosted inferences with poor resource request estimation results in latency Service Level Agreement (SLA) violation and below par
performance with higher end-to-end (E2E) delays. Lifetime static resource provisioning either hurts user experience for under-resource
provisioning or incurs cost with over-provisioning. Dynamic scaling offers to remedy delay by upscaling leading to additional cost
whereas a simple migration to another location offering latency in SLA bounds can reduce delay and minimize cost. To address this
cost and delay challenges for ML inferences in the inherent heterogeneous, resource-constrained, and distributed edge environment,
we propose ProKube which is a proactive container scaling and migration orchestrator to dynamically adjust the resources and
container locations with a fair balance between cost and delay. ProKube is built in conjunction with Google Kubernetes Engine (GKE)
enabling cross-cluster migration and/ or dynamic scaling. It further supports the regular addition of freshly collected logs into scheduling
decisions to handle unpredictable network behavior. Experiments conducted in heterogeneous edge settings show the efficacy of
ProKube to its counterparts Cost Greedy (CG), Latency Greedy (LG), and GeKube (GK). ProKube offers 68%, 7%, and 64% SLA
violation reduction to CG, LG, and GK, respectively. It improves cost by 4.77 cores to LG and offers more cost of 3.94 to CG and GK.


<p align="center">
  <img src="https://github.com/BabarAli93/ProKube/assets/50677432/25ee7984-03b1-477d-bf36-7a6869573143" alt="prokube">
</p>
