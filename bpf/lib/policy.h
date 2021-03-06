/*
 *  Copyright (C) 2016-2017 Authors of Cilium
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
#ifndef __LIB_POLICY_H_
#define __LIB_POLICY_H_

#include "drop.h"
#include "eps.h"
#include "maps.h"

#if defined POLICY_INGRESS || defined POLICY_EGRESS
#define REQUIRES_CAN_ACCESS
#endif

#ifdef REQUIRES_CAN_ACCESS

static inline int __inline__
__policy_can_access(void *map, struct __sk_buff *skb, __u32 identity,
		    __u16 dport, __u8 proto, size_t cidr_addr_size,
		    void *cidr_addr, int dir)
{
#ifdef DROP_ALL
	return DROP_POLICY;
#else
	struct policy_entry *policy;

	struct policy_key key = {
		.sec_label = identity,
		.dport = dport,
		.protocol = proto,
		.egress = !dir,
		.pad = 0,
	};

#ifdef HAVE_L4_POLICY
	policy = map_lookup_elem(map, &key);
	if (likely(policy)) {
		cilium_dbg3(skb, DBG_L4_CREATE, identity, SECLABEL,
			    dport << 16 | proto);

		/* FIXME: Use per cpu counters */
		__sync_fetch_and_add(&policy->packets, 1);
		__sync_fetch_and_add(&policy->bytes, skb->len);
		goto get_proxy_port;
	}
#endif /* HAVE_L4_POLICY */

	/* If L4 policy check misses, fall back to L3. */
	key.dport = 0;
	key.protocol = 0;
	policy = map_lookup_elem(map, &key);
	if (likely(policy)) {
		/* FIXME: Use per cpu counters */
		__sync_fetch_and_add(&policy->packets, 1);
		__sync_fetch_and_add(&policy->bytes, skb->len);
		return TC_ACT_OK;
	}

#ifdef HAVE_L4_POLICY
	key.sec_label = 0;
	key.dport = dport;
	key.protocol = proto;
	policy = map_lookup_elem(map, &key);
	if (likely(policy)) {
		/* FIXME: Use per cpu counters */
		__sync_fetch_and_add(&policy->packets, 1);
		__sync_fetch_and_add(&policy->bytes, skb->len);
		goto get_proxy_port;
	}
#endif /* HAVE_L4_POLICY */

	if (skb->cb[CB_POLICY])
		goto allow;

	return DROP_POLICY;
#ifdef HAVE_L4_POLICY
get_proxy_port:
	if (likely(policy)) {
		if (policy->proxy_port)
			return policy->proxy_port;
		else
			return l4_policy_lookup(skb, proto, dport, dir, false);
	}
#endif /* HAVE_L4_POLICY */
allow:
	return TC_ACT_OK;
#endif /* DROP_ALL */
}

/**
 * Determine whether the policy allows this traffic on ingress.
 * @arg skb		Packet to allow or deny
 * @arg src_identity	Source security identity for this packet
 * @arg dport		Destination port of this packet
 * @arg proto		L3 Protocol of this packet
 * @arg cidr_addr_size	Size of the destination CIDR of this packet
 * @arg cidr_addr	Destination CIDR of this packet
 *
 * Returns:
 *   - Positive integer indicating the proxy_port to handle this traffic
 *   - TC_ACT_OK if the policy allows this traffic based only on labels/L3/L4
 *   - Negative error code if the packet should be dropped
 */
static inline int __inline__
policy_can_access_ingress(struct __sk_buff *skb, __u32 src_identity,
			  __u16 dport, __u8 proto, size_t cidr_addr_size,
			  void *cidr_addr)
{
#ifdef DROP_ALL
	return DROP_POLICY;
#else
	int ret = __policy_can_access(&POLICY_MAP, skb, src_identity, dport,
				      proto, cidr_addr_size, cidr_addr,
				      CT_INGRESS);
	if (ret >= TC_ACT_OK)
		return ret;

	// cidr_addr_size is a compile time constant so this should all be inlined neatly.
	if (cidr_addr_size == sizeof(union v6addr) && lpm6_ingress_lookup(cidr_addr))
		goto allow;
	if (cidr_addr_size == sizeof(__be32) && lpm4_ingress_lookup(*(__be32 *)cidr_addr))
		goto allow;

	cilium_dbg(skb, DBG_POLICY_DENIED, src_identity, SECLABEL);

#ifndef IGNORE_DROP
	return DROP_POLICY;
#else
	ret = TC_ACT_OK;
#endif

allow:
	return TC_ACT_OK;
#endif /* DROP_ALL */
}

#else /* POLICY_INGRESS || REQUIRES_CAN_ACCESS */

static inline int
policy_can_access_ingress(struct __sk_buff *skb, __u32 src_label,
			  __u16 dport, __u8 proto, size_t cidr_addr_size,
			  void *cidr_addr)
{
	return TC_ACT_OK;
}

#endif /* POLICY_INGRESS || REQUIRES_CAN_ACCESS */

#if defined POLICY_EGRESS && defined LXC_ID

static inline int __inline__
policy_can_egress(struct __sk_buff *skb, __u16 identity, __u16 dport, __u8 proto)
{
#ifdef DROP_ALL
	return DROP_POLICY;
#else
	if (__policy_can_access(&POLICY_MAP, skb, identity, dport, proto, 0,
				NULL, CT_EGRESS) == TC_ACT_OK)
		goto allow;

	/* FIXME GH-1488: Remove this call when userspace pushes down
	 *		  label-dependent L4 policies. */
	int ret = l4_policy_lookup(skb, proto, dport, CT_EGRESS, false);
	if (ret >= 0)
		return ret;

	cilium_dbg(skb, DBG_POLICY_DENIED, identity, SECLABEL);
#ifndef IGNORE_DROP
	return DROP_POLICY;
#endif

allow:
	return TC_ACT_OK;
#endif /* DROP_ALL */
}

static inline int policy_can_egress6(struct __sk_buff *skb,
				     struct ipv6_ct_tuple *tuple)
{
	struct remote_endpoint_info *info;
	__u16 identity = 0;

	info = lookup_ip6_remote_endpoint(&tuple->daddr);
	if (info)
		identity = info->sec_label;
	else
		cilium_dbg(skb, DBG_IP_ID_MAP_FAILED6, tuple->daddr.p4, 0);

	return policy_can_egress(skb, identity, tuple->dport, tuple->nexthdr);
}

static inline int policy_can_egress4(struct __sk_buff *skb,
				     struct ipv4_ct_tuple *tuple)
{
	struct remote_endpoint_info *info;
	__u16 identity = 0;

	info = lookup_ip4_remote_endpoint(tuple->daddr);
	if (info)
		identity = info->sec_label;
	else
		cilium_dbg(skb, DBG_IP_ID_MAP_FAILED4, tuple->daddr, 0);

	return policy_can_egress(skb, identity, tuple->dport, tuple->nexthdr);
}

#else /* POLICY_EGRESS && LXC_ID */

static inline int
policy_can_egress6(struct __sk_buff *skb, struct ipv6_ct_tuple *tuple)
{
	return TC_ACT_OK;
}

static inline int
policy_can_egress4(struct __sk_buff *skb, struct ipv4_ct_tuple *tuple)
{
	return TC_ACT_OK;
}
#endif /* POLICY_EGRESS && LXC_ID */

#if defined POLICY_INGRESS || defined POLICY_EGRESS

/**
 * Mark skb to skip policy enforcement
 * @arg skb	packet
 *
 * Will cause the packet to ignore the policy enforcement layer and
 * be considered accepted despite of the policy outcome.
 */
static inline void policy_mark_skip(struct __sk_buff *skb)
{
	skb->cb[CB_POLICY] = 1;
}

static inline void policy_clear_mark(struct __sk_buff *skb)
{
	skb->cb[CB_POLICY] = 0;
}

static inline int is_policy_skip(struct __sk_buff *skb)
{
	return skb->cb[CB_POLICY];
}

#else /* POLICY_INGRESS || POLICY_EGRESS */


static inline void policy_mark_skip(struct __sk_buff *skb)
{
}

static inline void policy_clear_mark(struct __sk_buff *skb)
{
}

static inline int is_policy_skip(struct __sk_buff *skb)
{
	return 1;
}
#endif /* POLICY_INGRESS || POLICY_EGRESS */

#endif
