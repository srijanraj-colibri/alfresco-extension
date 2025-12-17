package com.example.alfresco.events;

import org.alfresco.model.ContentModel;
import org.alfresco.repo.node.NodeServicePolicies;
import org.alfresco.repo.policy.Behaviour;
import org.alfresco.repo.policy.JavaBehaviour;
import org.alfresco.repo.policy.PolicyComponent;
import org.alfresco.repo.transaction.AlfrescoTransactionSupport;
import org.alfresco.service.cmr.repository.ChildAssociationRef;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.namespace.QName;
import org.alfresco.util.PropertyCheck;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UploadEventBehaviour
        implements NodeServicePolicies.OnCreateNodePolicy,
                   NodeServicePolicies.OnUpdatePropertiesPolicy,
                   NodeServicePolicies.BeforeDeleteNodePolicy {

    private static final Log LOGGER =
            LogFactory.getLog(UploadEventBehaviour.class);

    private static final String TX_PROCESSED_CONTENT = "TX_PROCESSED_CONTENT";

    private PolicyComponent policyComponent;
    private NodeService nodeService;
    private ActiveMQPublisher publisher;

    /* ---------- Audit properties ---------- */
    private static final Set<QName> AUDIT_PROPS = Set.of(
            ContentModel.PROP_MODIFIED,
            ContentModel.PROP_MODIFIER,
            ContentModel.PROP_ACCESSED,
            ContentModel.PROP_CREATED,
            ContentModel.PROP_CREATOR
    );

    /* ---------- Spring setters ---------- */

    public void setPolicyComponent(PolicyComponent policyComponent) {
        this.policyComponent = policyComponent;
    }

    public void setNodeService(NodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setPublisher(ActiveMQPublisher publisher) {
        this.publisher = publisher;
    }

    /* ---------- Init ---------- */

    public void init() {
        PropertyCheck.mandatory(this, "policyComponent", policyComponent);
        PropertyCheck.mandatory(this, "nodeService", nodeService);
        PropertyCheck.mandatory(this, "publisher", publisher);

        Behaviour create = new JavaBehaviour(
                this,
                "onCreateNode",
                Behaviour.NotificationFrequency.TRANSACTION_COMMIT
        );

        Behaviour update = new JavaBehaviour(
                this,
                "onUpdateProperties",
                Behaviour.NotificationFrequency.TRANSACTION_COMMIT
        );

        Behaviour delete = new JavaBehaviour(
                this,
                "beforeDeleteNode",
                Behaviour.NotificationFrequency.TRANSACTION_COMMIT
        );

        policyComponent.bindClassBehaviour(
                NodeServicePolicies.OnCreateNodePolicy.QNAME,
                ContentModel.TYPE_CONTENT,
                create
        );

        policyComponent.bindClassBehaviour(
                NodeServicePolicies.OnUpdatePropertiesPolicy.QNAME,
                ContentModel.TYPE_CONTENT,
                update
        );

        policyComponent.bindClassBehaviour(
                NodeServicePolicies.BeforeDeleteNodePolicy.QNAME,
                ContentModel.TYPE_CONTENT,
                delete
        );

        LOGGER.info("🔥 UploadEventBehaviour INITIALIZED");
    }

    /* ---------- CREATE ---------- */

    @Override
    public void onCreateNode(ChildAssociationRef childAssocRef) {
        NodeRef nodeRef = childAssocRef.getChildRef();
        LOGGER.info("NODE_CREATED: " + nodeRef);
        publisher.publish("NODE_CREATED", nodeRef);
    }

    /* ---------- UPDATE ---------- */

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void onUpdateProperties(
            NodeRef nodeRef,
            Map before,
            Map after) {

        if (nodeRef == null || !nodeService.exists(nodeRef)) {
            return;
        }

        // Ignore version store & other stores
        if (!nodeRef.getStoreRef().equals(
                org.alfresco.service.cmr.repository.StoreRef.STORE_REF_WORKSPACE_SPACESSTORE)) {
            return;
        }

        Object beforeContent = before.get(ContentModel.PROP_CONTENT);
        Object afterContent  = after.get(ContentModel.PROP_CONTENT);

        /* ---------- CONTENT CHANGE ---------- */
        if ((beforeContent == null && afterContent != null) ||
            (beforeContent != null && !beforeContent.equals(afterContent))) {

            // Ensure content is fully written
            if (afterContent != null &&
                nodeService.hasAspect(nodeRef, ContentModel.ASPECT_VERSIONABLE)) {

                Set<NodeRef> processed =
                        (Set<NodeRef>) AlfrescoTransactionSupport.getResource(TX_PROCESSED_CONTENT);

                if (processed == null) {
                    processed = new HashSet<>();
                    AlfrescoTransactionSupport.bindResource(TX_PROCESSED_CONTENT, processed);
                }

                if (processed.add(nodeRef)) {
                    LOGGER.info("CONTENT_READY: " + nodeRef);
                    publisher.publish("CONTENT_READY", nodeRef);
                }
            }
            return;
        }

        /* ---------- METADATA CLASSIFICATION ---------- */
        Set<QName> changedProps = new HashSet<>(after.keySet());
        changedProps.removeAll(before.keySet());

        if (!changedProps.isEmpty()) {

            if (AUDIT_PROPS.containsAll(changedProps)) {
                LOGGER.info("AUDIT_UPDATED: " + nodeRef);
                publisher.publish("AUDIT_UPDATED", nodeRef);
            } else {
                LOGGER.info("METADATA_CHANGED: " + nodeRef);
                publisher.publish("METADATA_CHANGED", nodeRef);
            }
        }
    }

    /* ---------- DELETE ---------- */

    @Override
    public void beforeDeleteNode(NodeRef nodeRef) {
        LOGGER.info("NODE_DELETED: " + nodeRef);
        publisher.publish("NODE_DELETED", nodeRef);
    }
}
