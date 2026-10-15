//! Zone 2 CSR bidirectional graph and ISA-95 Euler Tour interval tree for O(1) subtree rollups.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 16-byte aligned Euler Tour interval node (`[tin, tout]`) for ISA-95 hierarchies (`location` and `asset`).
#[repr(C, align(16))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EulerTreeNode {
    pub tin: u32,
    pub tout: u32,
    pub effective_cost_center_id: u32,
    pub depth: u16,
    pub flags: u16, // Bit 0: has_children
}

impl EulerTreeNode {
    /// Returns true if `other` belongs to the subtree rooted at `self` (including `self`) in $O(1)$.
    #[inline]
    pub fn contains_in_subtree(&self, other: &EulerTreeNode) -> bool {
        self.tin <= other.tin && other.tin <= self.tout
    }

    #[inline]
    pub fn has_children(&self) -> bool {
        (self.flags & 1) != 0
    }
}

/// Zone 2 CSR Graph + ISA-95 Hierarchy Engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CsrGraphZone {
    pub euler_nodes: HashMap<u32, EulerTreeNode>,
    /// Forward edges: `(source_row_id, attr_id) -> Vec<target_row_id>`
    pub forward_csr: HashMap<(u32, u16), Vec<u32>>,
    /// Reverse VAET edges: `(target_row_id, attr_id) -> Vec<source_row_id>`
    pub reverse_vaet_csr: HashMap<(u32, u16), Vec<u32>>,
}

impl CsrGraphZone {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a reference edge and automatically indexes its inverse VAET edge.
    pub fn add_reference_edge(&mut self, source_row: u32, attr_id: u16, target_row: u32) {
        self.forward_csr
            .entry((source_row, attr_id))
            .or_default()
            .push(target_row);
        self.reverse_vaet_csr
            .entry((target_row, attr_id))
            .or_default()
            .push(source_row);
    }

    /// Returns all entities referencing `target_row` via `attr_id` in $O(1)$ slice lookup (replaces DynamoDB GSI-VAET).
    pub fn reverse_referrers(&self, target_row: u32, attr_id: u16) -> &[u32] {
        self.reverse_vaet_csr
            .get(&(target_row, attr_id))
            .map_or(&[], Vec::as_slice)
    }

    /// Compiles an ISA-95 hierarchy tree from `(node_id, Option<parent_id>, Option<own_cost_center_id>)`
    /// into $O(1)$ Euler Tour intervals `[tin, tout]` with automatic top-down `cost_center` inheritance.
    pub fn build_isa95_euler_tree(&mut self, nodes: &[(u32, Option<u32>, Option<u32>)]) {
        let mut children_map: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut own_cc_map: HashMap<u32, Option<u32>> = HashMap::new();
        let mut roots = Vec::new();

        for &(node_id, parent_opt, cc_opt) in nodes {
            own_cc_map.insert(node_id, cc_opt);
            if let Some(parent_id) = parent_opt {
                children_map.entry(parent_id).or_default().push(node_id);
            } else {
                roots.push(node_id);
            }
        }
        roots.sort_unstable();
        for ch in children_map.values_mut() {
            ch.sort_unstable();
        }

        let mut timer: u32 = 1;
        for root in roots {
            self.dfs_euler(root, 0, 0, &children_map, &own_cc_map, &mut timer);
        }
    }

    fn dfs_euler(
        &mut self,
        u: u32,
        depth: u16,
        inherited_cc: u32,
        children_map: &HashMap<u32, Vec<u32>>,
        own_cc_map: &HashMap<u32, Option<u32>>,
        timer: &mut u32,
    ) {
        let tin = *timer;
        *timer += 1;
        let effective_cc = own_cc_map
            .get(&u)
            .and_then(|&cc| cc)
            .unwrap_or(inherited_cc);
        let children = children_map.get(&u).cloned().unwrap_or_default();
        let has_children = !children.is_empty();

        for v in children {
            self.dfs_euler(v, depth + 1, effective_cc, children_map, own_cc_map, timer);
        }
        let tout = *timer - 1;
        self.euler_nodes.insert(
            u,
            EulerTreeNode {
                tin,
                tout,
                effective_cost_center_id: effective_cc,
                depth,
                flags: u16::from(has_children),
            },
        );
    }

    /// Filters all descendant node IDs in the subtree rooted at `root_id` using a single integer range check `[tin..=tout]`.
    pub fn filter_subtree_descendants(&self, root_id: u32) -> Vec<u32> {
        let Some(root_euler) = self.euler_nodes.get(&root_id) else {
            return Vec::new();
        };
        let mut out: Vec<u32> = self
            .euler_nodes
            .iter()
            .filter(|(_, node)| root_euler.contains_in_subtree(node))
            .map(|(&id, _)| id)
            .collect();
        out.sort_unstable();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isa95_euler_tree_resolves_5_level_subtrees_and_inherits_cost_centers_in_o1() {
        assert_eq!(std::mem::size_of::<EulerTreeNode>(), 16);
        assert_eq!(std::mem::align_of::<EulerTreeNode>(), 16);

        let mut graph = CsrGraphZone::new();
        // Level 0: Mall Root (ID 1, Cost Center 100)
        // Level 1: North Tower (ID 2, inherits 100), South Tower (ID 3, overrides with CC 200)
        // Level 2: HVAC Room (ID 4 under North Tower), Escalator Bank (ID 5 under South Tower)
        // Level 3: Chiller Trane #1 (ID 6 under HVAC Room), Compressor Sub-unit (ID 7 under Chiller #1)
        let hierarchy = vec![
            (1, None, Some(100)),
            (2, Some(1), None),
            (3, Some(1), Some(200)),
            (4, Some(2), None),
            (5, Some(3), None),
            (6, Some(4), None),
            (7, Some(6), None),
        ];
        graph.build_isa95_euler_tree(&hierarchy);

        // Subtree of North Tower (2) must be [2, 4, 6, 7] in O(1)
        assert_eq!(graph.filter_subtree_descendants(2), vec![2, 4, 6, 7]);
        // Subtree of Root (1) must be all 7 nodes
        assert_eq!(
            graph.filter_subtree_descendants(1),
            vec![1, 2, 3, 4, 5, 6, 7]
        );
        // Deepest node 7 must have inherited Cost Center 100 from Root (1), and node 5 must have inherited 200 from (3)
        assert_eq!(graph.euler_nodes[&7].effective_cost_center_id, 100);
        assert_eq!(graph.euler_nodes[&5].effective_cost_center_id, 200);
    }
}
