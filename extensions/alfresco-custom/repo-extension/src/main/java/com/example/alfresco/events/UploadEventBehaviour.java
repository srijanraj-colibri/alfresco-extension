package com.example.alfresco.events;

import org.alfresco.repo.node.NodeServicePolicies;
import org.alfresco.repo.policy.Behaviour;
import org.alfresco.repo.policy.JavaBehaviour;
import org.alfresco.repo.policy.PolicyComponent;
import org.alfresco.service.cmr.repository.ChildAssociationRef;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.model.ContentModel;
import org.alfresco.util.PropertyCheck;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UploadEventBehaviour
        implements NodeServicePolicies.OnCreateNodePolicy,
                   NodeServicePolicies.OnUpdatePropertiesPolicy,
                   NodeServicePolicies.BeforeDeleteNodePolicy {

    private static final Log LOGGER =
            LogFactory.getLog(UploadEventBehaviour.class);

    static {
        System.out.println("🔥🔥🔥 UploadEventBehaviour CLASS LOADED 🔥🔥🔥");
    }

    private PolicyComponent policyComponent;
    private NodeService nodeService;
    private ActiveMQPublisher publisher;

    public void setPolicyComponent(PolicyComponent policyComponent) {
        this.policyComponent = policyComponent;
    }

    public void setNodeService(NodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setPublisher(ActiveMQPublisher publisher) {
        this.publisher = publisher;
    }

    public void init() {
        PropertyCheck.mandatory(this, "policyComponent", policyComponent);
        PropertyCheck.mandatory(this, "nodeService", nodeService);
        PropertyCheck.mandatory(this, "publisher", publisher);

        LOGGER.info("🔥 UploadEventBehaviour INIT");

        Behaviour create = new JavaBehaviour(
                this, "onCreateNode", Behaviour.NotificationFrequency.TRANSACTION_COMMIT);

        Behaviour update = new JavaBehaviour(
                this, "onUpdateProperties", Behaviour.NotificationFrequency.TRANSACTION_COMMIT);

        Behaviour delete = new JavaBehaviour(
                this, "beforeDeleteNode", Behaviour.NotificationFrequency.TRANSACTION_COMMIT);

        policyComponent.bindClassBehaviour(
                NodeServicePolicies.OnCreateNodePolicy.QNAME,
                ContentModel.TYPE_CONTENT,
                create);

        policyComponent.bindClassBehaviour(
                NodeServicePolicies.OnUpdatePropertiesPolicy.QNAME,
                ContentModel.TYPE_CONTENT,
                update);

        policyComponent.bindClassBehaviour(
                NodeServicePolicies.BeforeDeleteNodePolicy.QNAME,
                ContentModel.TYPE_CONTENT,
                delete);

        LOGGER.info("🔥 UploadEventBehaviour BOUND TO POLICIES");
    }

    @Override
    public void onCreateNode(ChildAssociationRef childAssocRef) {
        NodeRef nodeRef = childAssocRef.getChildRef();
        LOGGER.info("📤 CREATE event: " + nodeRef);
        publisher.publish("CREATE", nodeRef);
    }

    @Override
    public void onUpdateProperties(
            NodeRef nodeRef,
            java.util.Map before,
            java.util.Map after) {

        LOGGER.info("✏ UPDATE event: " + nodeRef);
        publisher.publish("UPDATE", nodeRef);
    }

    @Override
    public void beforeDeleteNode(NodeRef nodeRef) {
        LOGGER.info("🗑 DELETE event: " + nodeRef);
        publisher.publish("DELETE", nodeRef);
    }
}
