# ProKube

Deep Neural Network (DNN) and Machine Learning (ML) models/ inferences are producing highly accurate results
demanding enormous computational resources. Limited on-premise resource of end-users gadgets drive to hosting inferences at
user-facing locations in edge to cloud continuum with users require fast responses. Kubernetes hosted inferences with poor resource
request estimation results in latency Service Level Agreement (SLA) violation and below par performance with higher end to end (E2E)
delays. Lifetime static resource provisioning either hurt user experience for under resource provisioning or incur cost with
over-provisioning. Dynamic scaling offer to remedy delay by upscaling leading to additional cost where a simple migration to a location
offering latency in SLA bounds can reduce delay and minimize cost. Default Kubernetes scheduler works on load balancing principle
where the Kubernetes pods are equally distributed among the cluster nodes based on resource availability with no consideration of
latency sensitivity and migration support. To address this cost and delay challenges for ML inferences in the inherent heterogeneous,
resource constrained and distributed edge environment, we propose ProKube which is a proactive container scaling and migration
orchestrator to dynamically adjust the resources and container locations with a fair balance between cost and delay. ProKube is built in
conjunction with Google Kubernetes Engine (GKE) enabling cross-cluster migration and/ or dynamic scaling. It further provisions
support for regular addition of freshly collected logs into scheduling decision to handle the unpredictable network behaviour.
Experiments conducted in heterogeneous edge settings show efficacy of ProKube to its counterparts in terms of lesser SLA violation
rate, cost and delay.



![prokube](https://github.com/BabarAli93/ProKube/assets/50677432/25ee7984-03b1-477d-bf36-7a6869573143)
