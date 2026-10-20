//! Grafo bidireccional CSR de la Zona 2 y árbol de intervalos jerárquico Euler Tour para agregaciones de subárboles en $O(1)$.

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::DomainResult;
use crate::mbf::format::pad_to_64;

const ZONE2_MAGIC: [u8; 8] = *b"MBFZ2CSR";

/// Nodo de intervalo Euler Tour alineado a 16 bytes (`[tin, tout]`) para jerarquías autorreferenciales arbitrarias.
#[repr(C, align(16))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
pub struct EulerTreeNode {
    /// Instante de entrada (`tin`) del recorrido en profundidad (DFS) de Euler.
    pub tin: u32,
    /// Instante de salida (`tout`) del recorrido en profundidad (DFS) de Euler.
    pub tout: u32,
    /// Identificador de atributo heredado de forma descendente en la jerarquía.
    pub inherited_attribute_id: u32,
    /// Profundidad del nodo respecto a la raíz del árbol.
    pub depth: u16,
    /// Banderas de estado del nodo (Bit 0: indica si posee nodos hijos).
    pub flags: u16, // Bit 0: has_children
}

impl EulerTreeNode {
    /// Devuelve `true` si `other` pertenece al subárbol enraizado en `self` (incluyendo a `self`) en $O(1)$.
    #[inline]
    pub fn contains_in_subtree(&self, other: &EulerTreeNode) -> bool {
        self.tin <= other.tin && other.tin <= self.tout
    }

    /// Devuelve `true` si el nodo posee al menos un nodo hijo directo.
    #[inline]
    pub fn has_children(&self) -> bool {
        (self.flags & 1) != 0
    }
}

/// Motor de grafo CSR y árbol jerárquico Euler Tour de la Zona 2.
/// Almacena tanto los índices dinámicos de adyacencia como los arreglos planos contiguos CSR (`fwd_offsets`, `fwd_targets`, `rev_offsets`, `rev_sources`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsrGraphZone {
    /// Mapa de nodos del árbol de intervalos Euler Tour por identificador de fila/nodo.
    pub euler_nodes: HashMap<u32, EulerTreeNode>,
    /// Aristas directas: `(source_row_id, attr_id) -> Vec<target_row_id>`.
    pub forward_csr: HashMap<(u32, u16), Vec<u32>>,
    /// Aristas inversas VAET: `(target_row_id, attr_id) -> Vec<source_row_id>`.
    pub reverse_vaet_csr: HashMap<(u32, u16), Vec<u32>>,
    /// Arreglo contiguo de desplazamientos de aristas salientes (Forward CSR).
    pub fwd_offsets: Vec<u32>,
    /// Arreglo contiguo de nodos destino de aristas salientes (Forward CSR).
    pub fwd_targets: Vec<u32>,
    /// Arreglo contiguo de identificadores de atributo de aristas salientes.
    pub fwd_attr_ids: Vec<u16>,
    /// Arreglo contiguo de desplazamientos de aristas entrantes (Reverse CSR).
    pub rev_offsets: Vec<u32>,
    /// Arreglo contiguo de nodos origen de aristas entrantes (Reverse CSR).
    pub rev_sources: Vec<u32>,
    /// Arreglo contiguo de identificadores de atributo de aristas entrantes.
    pub rev_attr_ids: Vec<u16>,
}

impl CsrGraphZone {
    /// Crea una nueva instancia vacía de `CsrGraphZone`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Añade una arista de referencia directa e indexa automáticamente su arista inversa VAET.
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

    /// Materializa los arreglos planos contiguos CSR (`fwd_offsets`, `fwd_targets`, `rev_offsets`, `rev_sources`).
    pub fn seal_contiguous_csr_arrays(&mut self) {
        let max_src = self
            .forward_csr
            .keys()
            .map(|&(s, _)| s as usize)
            .max()
            .unwrap_or(0);
        let max_dst = self
            .reverse_vaet_csr
            .keys()
            .map(|&(t, _)| t as usize)
            .max()
            .unwrap_or(0);
        let n_nodes = max_src.max(max_dst) + 1;

        let mut fwd_adj: Vec<Vec<(u32, u16)>> = vec![Vec::new(); n_nodes];
        for (&(src, attr), targets) in &self.forward_csr {
            if (src as usize) < n_nodes {
                for &t in targets {
                    fwd_adj[src as usize].push((t, attr));
                }
            }
        }
        self.fwd_offsets = Vec::with_capacity(n_nodes + 1);
        self.fwd_targets.clear();
        self.fwd_attr_ids.clear();
        self.fwd_offsets.push(0);
        for edges in &mut fwd_adj {
            edges.sort_unstable();
            for &(t, attr) in edges.iter() {
                self.fwd_targets.push(t);
                self.fwd_attr_ids.push(attr);
            }
            self.fwd_offsets.push(self.fwd_targets.len() as u32);
        }

        let mut rev_adj: Vec<Vec<(u32, u16)>> = vec![Vec::new(); n_nodes];
        for (&(dst, attr), sources) in &self.reverse_vaet_csr {
            if (dst as usize) < n_nodes {
                for &s in sources {
                    rev_adj[dst as usize].push((s, attr));
                }
            }
        }
        self.rev_offsets = Vec::with_capacity(n_nodes + 1);
        self.rev_sources.clear();
        self.rev_attr_ids.clear();
        self.rev_offsets.push(0);
        for edges in &mut rev_adj {
            edges.sort_unstable();
            for &(s, attr) in edges.iter() {
                self.rev_sources.push(s);
                self.rev_attr_ids.push(attr);
            }
            self.rev_offsets.push(self.rev_sources.len() as u32);
        }
    }

    /// Devuelve todas las entidades que referencian a `target_row` mediante `attr_id` en tiempo $O(1)$.
    pub fn reverse_referrers(&self, target_row: u32, attr_id: u16) -> &[u32] {
        self.reverse_vaet_csr
            .get(&(target_row, attr_id))
            .map_or(&[], Vec::as_slice)
    }

    /// Compila un árbol jerárquico autorreferencial a partir de tuplas `(node_id, Option<parent_id>, Option<own_inherited_attr_id>)`
    /// en intervalos Euler Tour `[tin, tout]` con herencia descendente automática de atributos.
    pub fn build_hierarchical_euler_tree(&mut self, nodes: &[(u32, Option<u32>, Option<u32>)]) {
        let mut children_map: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut own_attr_map: HashMap<u32, Option<u32>> = HashMap::new();
        let mut roots = Vec::new();

        for &(node_id, parent_opt, attr_opt) in nodes {
            own_attr_map.insert(node_id, attr_opt);
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
            self.dfs_euler(root, 0, 0, &children_map, &own_attr_map, &mut timer);
        }
    }

    fn dfs_euler(
        &mut self,
        u: u32,
        depth: u16,
        inherited_attr: u32,
        children_map: &HashMap<u32, Vec<u32>>,
        own_attr_map: &HashMap<u32, Option<u32>>,
        timer: &mut u32,
    ) {
        let tin = *timer;
        *timer += 1;
        let effective_attr = own_attr_map
            .get(&u)
            .and_then(|&val| val)
            .unwrap_or(inherited_attr);
        let children = children_map.get(&u).cloned().unwrap_or_default();
        let has_children = !children.is_empty();

        for v in children {
            self.dfs_euler(
                v,
                depth + 1,
                effective_attr,
                children_map,
                own_attr_map,
                timer,
            );
        }
        let tout = *timer - 1;
        self.euler_nodes.insert(
            u,
            EulerTreeNode {
                tin,
                tout,
                inherited_attribute_id: effective_attr,
                depth,
                flags: u16::from(has_children),
            },
        );
    }

    /// Filtra todos los identificadores de nodos descendientes en el subárbol enraizado en `root_id` mediante la comprobación de rango `[tin..=tout]`.
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

    /// Evalúa la pertenencia al intervalo de Euler `tin[u] <= row_tin <= tout[u]` sobre un corte columnar denso
    /// en lotes vectorizados de 16 carriles, devolviendo los índices de fila coincidentes en $O(N/16)$.
    pub fn filter_rows_in_subtree_vectorized(
        &self,
        root_id: u32,
        row_location_tins: &[u32],
    ) -> Vec<u32> {
        let Some(root_euler) = self.euler_nodes.get(&root_id) else {
            return Vec::new();
        };
        let tin_u = root_euler.tin;
        let tout_u = root_euler.tout;
        let mut matching_rows = Vec::new();

        for (chunk_idx, chunk) in row_location_tins.chunks(16).enumerate() {
            let base = (chunk_idx * 16) as u32;
            let mut lane_mask = 0u16;
            for (lane, &loc_tin) in chunk.iter().enumerate() {
                if loc_tin >= tin_u && loc_tin <= tout_u {
                    lane_mask |= 1u16 << lane;
                }
            }
            while lane_mask != 0 {
                let lane = lane_mask.trailing_zeros();
                matching_rows.push(base + lane);
                lane_mask &= lane_mask - 1;
            }
        }
        matching_rows
    }

    /// Codifica la Zona 2 en un bloque binario alineado a 64 bytes con cabecera `"MBFZ2CSR"`, nodos Euler Tour y aristas CSR.
    pub fn encode_zone_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ZONE2_MAGIC);

        // 1. Euler Tour Tree Slab
        let mut sorted_euler: Vec<(u32, EulerTreeNode)> =
            self.euler_nodes.iter().map(|(&k, &v)| (k, v)).collect();
        sorted_euler.sort_by_key(|&(id, _)| id);
        buf.extend_from_slice(&(sorted_euler.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // pad to 16B boundary before EulerTreeNode array
        for (node_id, node) in sorted_euler {
            buf.extend_from_slice(&node_id.to_le_bytes());
            buf.extend_from_slice(&[0u8; 12]); // align each entry header to 16B
            buf.extend_from_slice(bytemuck::bytes_of(&node));
        }

        // 2. Forward CSR Edge Table
        let mut sorted_fwd: Vec<((u32, u16), &Vec<u32>)> =
            self.forward_csr.iter().map(|(&k, v)| (k, v)).collect();
        sorted_fwd.sort_by_key(|&(k, _)| k);
        buf.extend_from_slice(&(sorted_fwd.len() as u32).to_le_bytes());
        for ((src, attr), targets) in sorted_fwd {
            buf.extend_from_slice(&src.to_le_bytes());
            buf.extend_from_slice(&attr.to_le_bytes());
            buf.extend_from_slice(&(targets.len() as u32).to_le_bytes());
            for &dst in targets {
                buf.extend_from_slice(&dst.to_le_bytes());
            }
        }

        pad_to_64(&mut buf);
        buf
    }

    /// Decodifica una instancia de `CsrGraphZone` desde un corte binario de la Zona 2 y reconstruye los índices CSR directos e inversos.
    pub fn decode_zone_binary(bytes: &[u8]) -> DomainResult<Self> {
        if bytes.len() < 16 || bytes[0..8] != ZONE2_MAGIC {
            return Err(DomainError::mbf(
                ErrorCode::MbfCorrupt001,
                "Invalid Zone 2 CSR binary magic header",
            ));
        }
        let mut cur = 8usize;
        let euler_count = read_u32(bytes, &mut cur)? as usize;
        let _pad = read_u32(bytes, &mut cur)?;
        let mut zone = Self::new();

        for _ in 0..euler_count {
            let node_id = read_u32(bytes, &mut cur)?;
            if cur + 12 + 16 > bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Unexpected EOF reading EulerTreeNode in Zone 2",
                ));
            }
            cur += 12;
            let node: EulerTreeNode = bytemuck::pod_read_unaligned(&bytes[cur..cur + 16]);
            cur += 16;
            zone.euler_nodes.insert(node_id, node);
        }

        let fwd_group_count = read_u32(bytes, &mut cur)? as usize;
        for _ in 0..fwd_group_count {
            let src = read_u32(bytes, &mut cur)?;
            if cur + 2 > bytes.len() {
                return Err(DomainError::mbf(
                    ErrorCode::MbfCorrupt001,
                    "Unexpected EOF reading attr_id in Zone 2",
                ));
            }
            let attr = u16::from_le_bytes([bytes[cur], bytes[cur + 1]]);
            cur += 2;
            let t_len = read_u32(bytes, &mut cur)? as usize;
            for _ in 0..t_len {
                let dst = read_u32(bytes, &mut cur)?;
                zone.add_reference_edge(src, attr, dst);
            }
        }
        zone.seal_contiguous_csr_arrays();
        Ok(zone)
    }
}

fn read_u32(bytes: &[u8], cur: &mut usize) -> DomainResult<u32> {
    if *cur + 4 > bytes.len() {
        return Err(DomainError::mbf(
            ErrorCode::MbfCorrupt001,
            "Unexpected EOF reading u32 in Zone 2",
        ));
    }
    let v = u32::from_le_bytes([
        bytes[*cur],
        bytes[*cur + 1],
        bytes[*cur + 2],
        bytes[*cur + 3],
    ]);
    *cur += 4;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hierarchical_euler_tree_and_csr_binary_roundtrip_resolve_subtrees_in_o1() {
        assert_eq!(std::mem::size_of::<EulerTreeNode>(), 16);
        assert_eq!(std::mem::align_of::<EulerTreeNode>(), 16);

        let mut graph = CsrGraphZone::new();
        let hierarchy = vec![
            (1, None, Some(100)),
            (2, Some(1), None),
            (3, Some(1), Some(200)),
            (4, Some(2), None),
            (5, Some(3), None),
            (6, Some(4), None),
            (7, Some(6), None),
        ];
        graph.build_hierarchical_euler_tree(&hierarchy);
        graph.add_reference_edge(10, 1, 6);
        graph.add_reference_edge(11, 1, 6);
        graph.seal_contiguous_csr_arrays();

        let bin = graph.encode_zone_binary();
        assert_eq!(bin.len() % 64, 0);
        assert_eq!(&bin[0..8], b"MBFZ2CSR");

        let restored = CsrGraphZone::decode_zone_binary(&bin).unwrap();

        assert_eq!(restored.filter_subtree_descendants(2), vec![2, 4, 6, 7]);
        assert_eq!(
            restored.filter_subtree_descendants(1),
            vec![1, 2, 3, 4, 5, 6, 7]
        );
        assert_eq!(restored.euler_nodes[&7].inherited_attribute_id, 100);
        assert_eq!(restored.euler_nodes[&5].inherited_attribute_id, 200);
        assert_eq!(restored.reverse_referrers(6, 1), &[10, 11]);

        let row_tins: Vec<u32> = (1..=7).map(|id| restored.euler_nodes[&id].tin).collect();
        // Subtree of node 2 contains nodes 2, 4, 6, 7 -> indices 1, 3, 5, 6 in `row_tins`
        assert_eq!(
            restored.filter_rows_in_subtree_vectorized(2, &row_tins),
            vec![1, 3, 5, 6]
        );
    }
}
